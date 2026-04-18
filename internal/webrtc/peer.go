// Package webrtc provides the Pion-based WebRTC peer that consumes packets
// forwarded by the PacketInterceptor and broadcasts them to connected viewers.
//
// Flow:
//
//	PacketInterceptor.out ──► PeerManager.consumePackets()
//	                                  │
//	                           pion VideoTrack.WriteSample()
//	                                  │
//	                         browser via WebRTC data-channel
package webrtc

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/pion/webrtc/v4"

	"huvio-ai/web_stream/internal/pipeline"
)

// ─────────────────────────────────────────────────────────────────────────────
// PeerManager
// ─────────────────────────────────────────────────────────────────────────────

// PeerManager holds all active WebRTC peer connections and a shared video track.
// It receives IngressPackets from the interceptor's output channel and writes
// their RTP payload to the track for all connected peers.
type PeerManager struct {
	mu      sync.RWMutex
	peers   map[string]*webrtc.PeerConnection
	track   *webrtc.TrackLocalStaticRTP
	api     *webrtc.API
	in      <-chan *pipeline.IngressPacket
	logger  *log.Logger
}

// NewPeerManager creates a PeerManager that reads from in.
// Call Run() in a goroutine to start forwarding packets.
func NewPeerManager(in <-chan *pipeline.IngressPacket) (*PeerManager, error) {
	// Build a Pion API with default codecs
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register codecs: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))

	// Shared H.264 video track — all peers subscribe to this
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264},
		"video", "huvio-stream",
	)
	if err != nil {
		return nil, fmt.Errorf("create video track: %w", err)
	}

	return &PeerManager{
		peers:  make(map[string]*webrtc.PeerConnection),
		track:  track,
		api:    api,
		in:     in,
		logger: log.New(os.Stdout, "[webrtc]      ", log.LstdFlags|log.Lmicroseconds),
	}, nil
}

// Run starts the packet-forwarding loop. It blocks until in is closed.
// Run in a dedicated goroutine.
func (pm *PeerManager) Run(stop <-chan struct{}) {
	pm.logger.Println("packet-forwarding loop started")
	for {
		select {
		case <-stop:
			pm.logger.Println("stop — closing all peer connections")
			pm.mu.Lock()
			for id, pc := range pm.peers {
				pc.Close()
				delete(pm.peers, id)
			}
			pm.mu.Unlock()
			return

		case pkt, ok := <-pm.in:
			if !ok {
				pm.logger.Println("input channel closed")
				return
			}
			pm.writePacket(pkt)
		}
	}
}

// writePacket marshals the intercepted RTP packet and writes it to the track.
func (pm *PeerManager) writePacket(pkt *pipeline.IngressPacket) {
	raw, err := pkt.RTP.Marshal()
	if err != nil {
		pm.logger.Printf("ERROR marshal RTP seq=%d: %v", pkt.Meta.SequenceNum, err)
		return
	}
	// WriteRTP sends the raw RTP bytes to all peer connections subscribed to the track.
	if _, err := pm.track.Write(raw); err != nil {
		// Ignore "use of closed network connection" — happens during teardown
		pm.logger.Printf("WARN track.Write seq=%d: %v", pkt.Meta.SequenceNum, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP signaling handlers
// ─────────────────────────────────────────────────────────────────────────────

// sdpRequest is the JSON body for an SDP offer/answer exchange.
type sdpRequest struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"` // "offer"
}

// HandleOffer creates a new PeerConnection, negotiates SDP, and returns the
// answer. Registered as POST /api/offer.
func (pm *PeerManager) HandleOffer(w http.ResponseWriter, r *http.Request) {
	var req sdpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	pcConfig := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}
	pc, err := pm.api.NewPeerConnection(pcConfig)
	if err != nil {
		pm.logger.Printf("ERROR new peer: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Attach the shared video track
	if _, err := pc.AddTrack(pm.track); err != nil {
		pm.logger.Printf("ERROR add track: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Register connection state handler
	peerID := r.RemoteAddr
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		pm.logger.Printf("peer %s state → %s", peerID, state)
		if state == webrtc.PeerConnectionStateClosed ||
			state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateDisconnected {
			pm.mu.Lock()
			delete(pm.peers, peerID)
			pm.mu.Unlock()
		}
	})

	// Set remote SDP
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  req.SDP,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		pm.logger.Printf("ERROR set remote SDP: %v", err)
		http.Error(w, "bad SDP", http.StatusBadRequest)
		return
	}

	// Create answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pm.logger.Printf("ERROR create answer: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	gatherDone := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		pm.logger.Printf("ERROR set local desc: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	<-gatherDone

	// Store peer
	pm.mu.Lock()
	pm.peers[peerID] = pc
	pm.mu.Unlock()
	pm.logger.Printf("new peer connected: %s", peerID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sdpRequest{ //nolint:errcheck
		SDP:  pc.LocalDescription().SDP,
		Type: "answer",
	})
}

// PeerCount returns the number of currently connected WebRTC peers.
// Used by the centralised makeStatsHandler in main.go.
func (pm *PeerManager) PeerCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.peers)
}

// HandleStats is kept for direct use if needed; the main stats endpoint
// uses makeStatsHandler which calls PeerCount() instead.
func (pm *PeerManager) HandleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"peers": pm.PeerCount(),
	})
}
