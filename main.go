// web_stream — Huvio AI packet pipeline server
//
// This server:
//  1. Captures video from a local webcam via GStreamer (CameraListener)
//  2. Passes every RTP datagram through the PacketInterceptor (hook point)
//  3. Groups RTP packets into H.264 Access Units and paces them via the
//     Frame-Aware Metronome (replaces old JitterBuffer + PTSNormalizer)
//  4. Forwards complete, timestamp-normalised frames to the WebRTC PeerManager
//  5. Serves an HTTP signaling endpoint for browser viewers
//
// Usage:
//
//	go run . [flags]
//
// Flags:
//
//	-http     HTTP signaling address (default :8080)
//	-buf      Interceptor output channel depth (default 256)
//	-fps      Target output frame rate (default 30)
//
// Pipeline diagram (Frame-Aware Metronome architecture):
//
//	GStreamer (v4l2src) ──UDP──► CameraListener.Listen()
//	                                    │
//	                                    ▼
//	                         PacketInterceptor.Intercept()   ← hook point
//	                         (latency-alert hook)
//	                                    │
//	                               rawCh (buffered)
//	                                    │
//	                                    ▼
//	                         FrameMetronome.Run()
//	                         ┌──────────────────────────────┐
//	                         │  Accumulate RTP → full AU    │
//	                         │  Linear PTS normalisation    │
//	                         │  33ms tick → burst all pkts  │
//	                         └──────────────────────────────┘
//	                                    │
//	                               webrtcCh (buffered)
//	                                    │
//	                                    ▼
//	                         PeerManager.Run()
//	                                    │
//	                          Pion VideoTrack.Write()
//	                                    │
//	                             browser WebRTC peer
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof" // register /debug/pprof handlers
	"os"
	"os/signal"
	"syscall"
	"time"

	"huvio-ai/web_stream/internal/pipeline"
	webrtcpeer "huvio-ai/web_stream/internal/webrtc"
)

func main() {
	var (
		httpAddr   = flag.String("http", ":8080", "HTTP signaling server address")
		bufDepth   = flag.Int("buf", 256, "Interceptor output channel buffer depth")
		targetFPS  = flag.Float64("fps", 30.0, "Target output frame rate for the metronome (Layer 2)")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "[web_stream]  ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("▶ Huvio AI web_stream starting")

	// Graceful-shutdown plumbing
	stop := make(chan struct{})
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit
		logger.Println("■ shutdown signal — stopping all components")
		close(stop)
	}()

	// ─── 1. Create the interceptor → jitter buffer channel ──────────────────
	// Packets that survive all interceptor hooks land here.
	rawCh := make(chan *pipeline.IngressPacket, *bufDepth)

	// ─── 2. Build the PacketInterceptor ──────────────────────────────────────
	// Central hook point. Built-in inspect hook is auto-registered.
	interceptor := pipeline.NewPacketInterceptor(rawCh)

	// Register optional built-in hooks:
	//   • Latency alert — warns when one-way delay exceeds 50 ms
	interceptor.AddHook("latency-alert",
		pipeline.LatencyAlertHook(50.0, log.New(os.Stdout, "[latency]     ", log.LstdFlags)))
	//
	// NOTE: the old "ts-rewrite" hook has been REMOVED.
	// Timestamp normalization now happens in the PTSNormalizer (Step 3) which
	// runs AFTER the jitter buffer. Rewriting timestamps at arrival time
	// (in the interceptor) was wrong because the browser sees the timestamp
	// relative to when the packet is transmitted, not when it arrived.

	// ─── 3. Build the Frame-Aware Metronome ──────────────────────────────────
	// webrtcCh carries complete frames (Access Units) to the PeerManager.
	// This replaces both the old JitterBuffer and PTSNormalizer.
	webrtcCh := make(chan *pipeline.IngressPacket, *bufDepth)
	fm := pipeline.NewFrameMetronome(pipeline.FrameMetronomeConfig{
		In:        rawCh,
		Out:       webrtcCh,
		TargetFPS: *targetFPS,
		ClockRate: 90000,
	})

	// ─── 4. Start the Metronome loop ─────────────────────────────────────────
	go fm.Run(stop)

	// ─── 5. Build the WebRTC PeerManager ─────────────────────────────────────
	// PeerManager now consumes from the frame-aware metronome output.
	pm, err := webrtcpeer.NewPeerManager(webrtcCh)
	if err != nil {
		logger.Fatalf("create peer manager: %v", err)
	}

	// ─── 6. Build the Camera Listener ─────────────────────────────────────────
	ingester := pipeline.NewCameraListener(interceptor)



	// ─── 9. pprof debug server (Test 2 — Resource Leak) ──────────────────────
	go func() {
		profAddr := ":6060"
		logger.Printf("pprof server on http://localhost%s/debug/pprof", profAddr)
		if err := http.ListenAndServe(profAddr, nil); err != nil {
			logger.Printf("pprof server error: %v", err)
		}
	}()

	// ─── 10. Start the WebRTC forwarding loop ─────────────────────────────────
	go pm.Run(stop)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/login", corsMiddleware(handleLogin))
	mux.HandleFunc("/api/offer", corsMiddleware(authMiddleware(pm.HandleOffer)))
	// /api/stats includes interceptor, frame metronome AND peer count metrics
	mux.HandleFunc("/api/stats", corsMiddleware(makeStatsHandler(pm, interceptor, fm)))
	mux.HandleFunc("/", serveViewer)

	srv := &http.Server{
		Addr:         *httpAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.Printf("HTTP signaling server on %s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http server: %v", err)
		}
	}()

	// Shutdown HTTP gracefully when stop fires
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx) //nolint:errcheck
	}()

	// ─── 12. Camera reconnect loop (Test 4 — Failover) ───────────────────────
	// If GStreamer exits (camera disconnected), we wait 3 s and restart.
	// Closing stop exits the loop cleanly.
	for {
		err := ingester.Listen(stop)
		select {
		case <-stop:
			logger.Printf("ingester stopped: %v", err)
			goto shutdown
		default:
			logger.Printf("[FAILOVER] camera source disconnected (%v) — reconnecting in 3s", err)
			time.Sleep(3 * time.Second)
			ingester = pipeline.NewCameraListener(interceptor)
		}
	}
shutdown:

	// Print final stats
	rx, dropped, fwd := interceptor.Stats()
	fs := fm.Stats()
	logger.Printf("interceptor  — received=%d dropped=%d forwarded=%d", rx, dropped, fwd)
	logger.Printf("frame metro  — frames_in=%d burst=%d pkts_out=%d force_flush=%d ts_fix=%d",
		fs.FramesAccumulated, fs.FramesBurst, fs.PacketsOut, fs.ForceFlushes, fs.TimestampFixes)
	logger.Println("■ web_stream exited cleanly")
}

// corsMiddleware allows cross-origin requests from the Next.js frontend
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	}
}

// authMiddleware enforces the mock Bearer token for protected routes
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || authHeader != "Bearer mock-prototype-token-123" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}
}

// handleLogin provides a mock authentication flow
func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
		"token": "mock-prototype-token-123",
	})
}

// makeStatsHandler returns a unified /api/stats handler combining
// interceptor counters, frame metronome health, and peer count.
func makeStatsHandler(
	pm interface{ PeerCount() int },
	interceptor interface{ Stats() (uint64, uint64, uint64) },
	fm interface{ Stats() pipeline.FrameMetronomeStats },
) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		rx, dropped, fwd := interceptor.Stats()
		fs := fm.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"interceptor": map[string]uint64{
				"received":  rx,
				"dropped":   dropped,
				"forwarded": fwd,
			},
			"frame_metronome": fs,
			"peers":           pm.PeerCount(),
		})
	}
}

// serveViewer returns a minimal HTML page that connects to the stream via WebRTC.
func serveViewer(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(viewerHTML)) //nolint:errcheck
}

const viewerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Huvio AI — Live Stream Viewer</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0a0a0f;
    color: #e2e8f0;
    font-family: 'Inter', system-ui, sans-serif;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    gap: 1.5rem;
  }
  h1 { font-size: 1.5rem; background: linear-gradient(135deg,#6366f1,#8b5cf6); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
  video { width: min(90vw,960px); aspect-ratio:16/9; background:#000; border-radius:12px; box-shadow:0 0 40px rgba(99,102,241,.3); }
  #status { font-size:.85rem; color:#94a3b8; }
  button { padding:.6rem 1.6rem; background:linear-gradient(135deg,#6366f1,#8b5cf6); color:#fff; border:none; border-radius:8px; cursor:pointer; font-size:1rem; }
  button:hover { opacity:.85; }
  #stats { font-family:monospace; font-size:.8rem; color:#64748b; }
</style>
</head>
<body>
  <h1>🎥 Huvio AI — Live Stream</h1>
  <video id="video" autoplay playsinline muted></video>
  <div id="status">Disconnected</div>
  <button onclick="connect()">Connect to Stream</button>
  <pre id="stats"></pre>

<script>
let pc = null;

async function connect() {
  if (pc) { pc.close(); pc = null; }
  setStatus('Connecting…');

  pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
  pc.ontrack = e => {
    document.getElementById('video').srcObject = e.streams[0];
    setStatus('▶ streaming');
  };
  pc.onconnectionstatechange = () => setStatus(pc.connectionState);

  pc.addTransceiver('video', { direction: 'recvonly' });

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);

  const authRes = await fetch('/api/auth/login', { method: 'POST' });
  const authData = await authRes.json();
  const token = authData.token;

  const res = await fetch('/api/offer', {
    method: 'POST',
    headers: { 
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + token
    },
    body: JSON.stringify({ sdp: offer.sdp, type: offer.type })
  });
  const answer = await res.json();
  await pc.setRemoteDescription({ type: answer.type, sdp: answer.sdp });

  // Poll stats
  setInterval(async () => {
    const s = await fetch('/api/stats').then(r=>r.json());
    document.getElementById('stats').textContent = JSON.stringify(s, null, 2);
  }, 2000);
}

function setStatus(msg) {
  document.getElementById('status').textContent = msg;
}
</script>
</body>
</html>`
