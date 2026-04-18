// Package pipeline — Linear PTS Normalizer
//
// PTSNormalizer is the final stage of the backend pipeline. It sits immediately
// after the JitterBuffer metronome and rewrites every outgoing packet's RTP
// Timestamp using strict linear normalization.
//
// WHY THIS STAGE EXISTS
// ─────────────────────
// After the JitterBuffer drains a packet, its RTP.Header.Timestamp still
// contains the value that was set at arrival time (or by the old wall-clock
// ts-rewrite hook). That value has nothing to do with WHEN the packet is
// actually transmitted. The gap between "what the timestamp says" and "when
// the packet arrives" is exactly what causes the browser decoder to
// micro-stutter — it tries to render the frame at the wrong moment.
//
// THE FIX — Linear Timestamp Normalization
// ─────────────────────────────────────────
// For every packet the metronome releases, we assign a new timestamp that is
// computed purely from the outgoing sequence, not the arrival time:
//
//	NewTimestamp = PreviousTimestamp + (ClockRate / TargetFPS)
//
// For a 90 kHz clock at 30 FPS this is exactly +3000 units per frame.
//
// RFC 3550 §5.1 compliance:
//   - The first packet of a session is assigned a cryptographically-random
//     uint32 offset. This prevents timestamp-based session linkage attacks.
//   - Every subsequent packet increments by exactly tsIncrement. There is no
//     drift, no wall-clock dependency, no jitter.
//
// POSITION IN THE PIPELINE
// ────────────────────────
//	JitterBuffer.smoothCh → PTSNormalizer.Run() → webrtcCh → PeerManager
//
// Running AFTER the metronome is the critical design choice: the timestamp is
// stamped at the exact moment of transmission, so what the browser receives is
// always self-consistent.
//
// USAGE
//
//	pn := pipeline.NewPTSNormalizer(pipeline.PTSConfig{
//	    In:        smoothCh,   // jitter buffer output
//	    Out:       webrtcCh,   // PeerManager input
//	    ClockRate: 90_000,     // standard H.264 RTP clock
//	    TargetFPS: 30.0,
//	})
//	go pn.Run(stop)
package pipeline

import (
	"crypto/rand"
	"encoding/binary"
	"log"
	"os"
	"sync/atomic"
)

// ─────────────────────────────────────────────────────────────────────────────
// PTSConfig
// ─────────────────────────────────────────────────────────────────────────────

// PTSConfig configures the PTSNormalizer.
type PTSConfig struct {
	// In is the jitter buffer output channel (smoothCh).
	In <-chan *IngressPacket

	// Out is the channel going to the PeerManager (webrtcCh).
	Out chan<- *IngressPacket

	// ClockRate is the RTP clock frequency in Hz.
	// Standard for H.264 per RFC 6184 §8.3 is 90,000 Hz.
	ClockRate uint32

	// TargetFPS is the frame rate the metronome is running at.
	// Combined with ClockRate it determines tsIncrement.
	TargetFPS float64
}

func (c *PTSConfig) applyDefaults() {
	if c.ClockRate == 0 {
		c.ClockRate = 90_000
	}
	if c.TargetFPS <= 0 {
		c.TargetFPS = 30.0
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PTSNormalizer
// ─────────────────────────────────────────────────────────────────────────────

// PTSNormalizer rewrites the RTP timestamp of every packet that leaves the
// jitter buffer, assigning a strictly-increasing linear value.
type PTSNormalizer struct {
	cfg         PTSConfig
	tsIncrement uint32  // = ClockRate / TargetFPS (e.g. 3000 for 30 FPS @ 90kHz)
	nextTS      uint32  // value assigned to the next outgoing packet
	initialized bool    // false until the first packet anchors nextTS
	logger      *log.Logger

	// Atomic stats
	totalNormalized uint64
}

// NewPTSNormalizer constructs a PTSNormalizer. Call Run() in a goroutine.
func NewPTSNormalizer(cfg PTSConfig) *PTSNormalizer {
	cfg.applyDefaults()
	inc := uint32(float64(cfg.ClockRate) / cfg.TargetFPS)
	return &PTSNormalizer{
		cfg:         cfg,
		tsIncrement: inc,
		logger:      log.New(os.Stdout, "[pts]         ", log.LstdFlags|log.Lmicroseconds),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Run — the normalizer loop
// ─────────────────────────────────────────────────────────────────────────────

// Run reads packets from cfg.In, rewrites their RTP Timestamp, and forwards
// them to cfg.Out. It blocks until stop is closed or cfg.In is closed.
func (pn *PTSNormalizer) Run(stop <-chan struct{}) {
	pn.logger.Printf("▶ PTS normalizer started  clockRate=%d  fps=%.2f  tsIncrement=%d",
		pn.cfg.ClockRate, pn.cfg.TargetFPS, pn.tsIncrement)

	for {
		select {
		case <-stop:
			pn.logger.Printf("■ stopped  normalized=%d", atomic.LoadUint64(&pn.totalNormalized))
			return

		case pkt, ok := <-pn.cfg.In:
			if !ok {
				pn.logger.Println("input channel closed")
				return
			}
			pn.normalize(pkt)
		}
	}
}

// normalize is called for each packet leaving the jitter buffer metronome.
// It assigns the next linear timestamp and increments the counter for the
// packet that will follow.
func (pn *PTSNormalizer) normalize(pkt *IngressPacket) {
	// ── First packet: seed with a cryptographically-random offset ─────────────
	// RFC 3550 §5.1: "The initial value of the timestamp SHOULD be chosen
	// randomly, as if it were just an ordinary timestamp value."
	if !pn.initialized {
		randomOffset := pn.cryptoRandUint32()
		pn.nextTS = randomOffset
		pn.initialized = true
		pn.logger.Printf("first packet  seq=%d  random_offset=%d  increment=%d",
			pkt.Meta.SequenceNum, randomOffset, pn.tsIncrement)
	}

	// ── Record what we're overwriting (for the log) ────────────────────────────
	originalTS := pkt.RTP.Header.Timestamp

	// ── Overwrite the RTP timestamp with the perfectly-linear value ───────────
	// This is the core of Linear Timestamp Normalization:
	//   NewTS = PrevTS + (ClockRate / TargetFPS)
	// The browser decoder will now see timestamps that match the actual
	// transmission cadence produced by the metronome — zero guesswork needed.
	pkt.RTP.Header.Timestamp = pn.nextTS

	// ── Advance for the next packet ───────────────────────────────────────────
	// uint32 wrap-around is intentional and correct per RFC 3550.
	pn.nextTS += pn.tsIncrement

	n := atomic.AddUint64(&pn.totalNormalized, 1)

	// Log every 30th packet and every key-frame.
	if pkt.Meta.IsKeyFrame || n%30 == 1 {
		keyTag := ""
		if pkt.Meta.IsKeyFrame {
			keyTag = " [KEY]"
		}
		pn.logger.Printf(
			"normalized  seq=%d  orig_ts=%-10d → new_ts=%-10d  Δts=%d%s",
			pkt.Meta.SequenceNum,
			originalTS,
			pkt.RTP.Header.Timestamp,
			pn.tsIncrement,
			keyTag,
		)
	}

	// ── Forward to WebRTC ─────────────────────────────────────────────────────
	select {
	case pn.cfg.Out <- pkt:
		// forwarded successfully
	default:
		// WebRTC layer not consuming — drop to protect timing guarantees.
		// A blocked normalizer would stall the jitter buffer's metronome.
		pn.logger.Printf("WARN out channel full — discarding seq=%d ts=%d",
			pkt.Meta.SequenceNum, pkt.RTP.Header.Timestamp)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// cryptoRandUint32 returns a cryptographically-random uint32 suitable for use
// as an RTP timestamp seed (RFC 3550 §5.1).
func (pn *PTSNormalizer) cryptoRandUint32() uint32 {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		// Fallback: use a fixed but non-zero seed rather than crashing.
		pn.logger.Printf("WARN crypto/rand failed: %v — using fallback seed", err)
		return 0xBEEF_0000
	}
	return binary.BigEndian.Uint32(buf)
}

// ─────────────────────────────────────────────────────────────────────────────
// PTSStats snapshot
// ─────────────────────────────────────────────────────────────────────────────

// PTSStats is a point-in-time snapshot of normalizer metrics.
type PTSStats struct {
	TotalNormalized uint64 `json:"total_normalized"`
	TSIncrement     uint32 `json:"ts_increment"`
	NextTS          uint32 `json:"next_ts"`
}

// Stats returns a non-locking snapshot of the normalizer state.
func (pn *PTSNormalizer) Stats() PTSStats {
	return PTSStats{
		TotalNormalized: atomic.LoadUint64(&pn.totalNormalized),
		TSIncrement:     pn.tsIncrement,
		NextTS:          pn.nextTS,
	}
}
