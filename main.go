// web_stream — Huvio AI packet pipeline server
//
// This server:
//  1. Binds a UDP socket to receive video packets from the simulator
//  2. Passes every datagram through the PacketInterceptor (hook point)
//  3. Reorders and paces packets through the Dual-Layer JitterBuffer
//  4. Forwards smoothly-timed packets to the WebRTC peer manager
//  5. Serves an HTTP signaling endpoint for browser viewers
//
// Usage:
//
//	go run . [flags]
//
// Flags:
//
//	-udp      UDP ingestion address (default :5004)
//	-http     HTTP signaling address (default :8080)
//	-buf      Interceptor output channel depth (default 256)
//	-window   Jitter reorder window in ms (default 60)
//	-fps      Target output frame rate (default 30)
//
// Pipeline diagram (STEP 3 — reliable PTS):
//
//	simulator ──UDP──► UDPIngester.Listen()
//	                         │
//	                         ▼
//	              PacketInterceptor.Intercept()   ← hook point
//	              (latency-alert hook only;
//	               ts-rewrite REMOVED — see PTSNormalizer)
//	                         │
//	                    rawCh (buffered)
//	                         │
//	                         ▼
//	              JitterBuffer.Run()              ← Step 2
//	              ┌──────────────────────────────┐
//	              │  Layer 1: min-heap reorder   │
//	              │  Layer 2: ticker drain       │
//	              └──────────────────────────────┘
//	                         │
//	                    smoothCh (buffered)
//	                         │
//	                         ▼
//	              PTSNormalizer.Run()             ← NEW (Step 3)
//	              ┌──────────────────────────────┐
//	              │  NewTS = PrevTS + (90k/FPS)  │
//	              │  RFC 3550 random seed        │
//	              └──────────────────────────────┘
//	                         │
//	                    webrtcCh (buffered)
//	                         │
//	                         ▼
//	              PeerManager.Run()
//	                         │
//	               Pion VideoTrack.Write()
//	                         │
//	                  browser WebRTC peer
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"huvio-ai/web_stream/internal/pipeline"
	webrtcpeer "huvio-ai/web_stream/internal/webrtc"
)

func main() {
	var (
		udpAddr    = flag.String("udp", ":5004", "UDP ingestion address (simulator sends here)")
		httpAddr   = flag.String("http", ":8080", "HTTP signaling server address")
		bufDepth   = flag.Int("buf", 256, "Interceptor output channel buffer depth")
		windowMs   = flag.Int("window", 60, "Jitter reorder window in milliseconds (Layer 1)")
		targetFPS  = flag.Float64("fps", 30.0, "Target output frame rate for the metronome (Layer 2)")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "[web_stream]  ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("▶ Huvio AI web_stream starting")

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

	// ─── 3. Build the Dual-Layer Jitter Buffer ────────────────────────────────
	// smoothCh carries metronome-paced but still un-normalised packets.
	smoothCh := make(chan *pipeline.IngressPacket, 64)

	tickInterval := time.Duration(float64(time.Second) / *targetFPS)
	jb := pipeline.NewJitterBuffer(pipeline.JitterConfig{
		In:             rawCh,
		Out:            smoothCh,
		WindowDuration: time.Duration(*windowMs) * time.Millisecond,
		TickInterval:   tickInterval,
	})
	logger.Printf("jitter buffer  window=%dms  tick=%v (~%.1f FPS)",
		*windowMs, tickInterval, *targetFPS)

	// ─── 4. Build the PTS Normalizer (Step 3) ─────────────────────────────────
	// webrtcCh carries packets with perfectly-linear RTP timestamps.
	// The PTSNormalizer runs AFTER the metronome so timestamps are stamped at
	// the exact moment of transmission — not at arrival.
	//
	// Math:  NewTS = PreviousTS + (90000 / TargetFPS)
	// 30fps: each packet increments timestamp by exactly 3000 units.
	// First packet: cryptographically-random seed (RFC 3550 §5.1).
	webrtcCh := make(chan *pipeline.IngressPacket, 64)
	pn := pipeline.NewPTSNormalizer(pipeline.PTSConfig{
		In:        smoothCh,
		Out:       webrtcCh,
		ClockRate: 90_000,
		TargetFPS: *targetFPS,
	})
	tsIncrement := uint32(90_000 / *targetFPS)
	logger.Printf("PTS normalizer  clockRate=90000  fps=%.1f  tsIncrement=%d",
		*targetFPS, tsIncrement)

	// ─── 5. Build the WebRTC PeerManager ─────────────────────────────────────
	// Now reads from webrtcCh — paced AND timestamp-normalised.
	pm, err := webrtcpeer.NewPeerManager(webrtcCh)
	if err != nil {
		logger.Fatalf("create peer manager: %v", err)
	}

	// ─── 6. Build the UDP Ingester ────────────────────────────────────────────
	ingester := pipeline.NewUDPIngester(*udpAddr, interceptor)

	// ─── 7. Graceful-shutdown plumbing ────────────────────────────────────────
	stop := make(chan struct{})
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit
		logger.Println("■ shutdown signal — stopping all components")
		close(stop)
	}()

	// ─── 8. Start the jitter buffer (Layer 1 + 2) ─────────────────────────────
	go jb.Run(stop)

	// ─── 9. Start the PTS normalizer ──────────────────────────────────────────
	go pn.Run(stop)

	// ─── 10. Start the WebRTC forwarding loop ─────────────────────────────────
	go pm.Run(stop)

	// ─── 11. Start the HTTP signaling server ──────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/api/offer", corsMiddleware(pm.HandleOffer))
	// /api/stats includes interceptor, jitter buffer AND PTS normalizer metrics
	mux.HandleFunc("/api/stats", corsMiddleware(makeStatsHandler(pm, interceptor, jb, pn)))
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

	// ─── 12. Start UDP ingestion (blocks until stop) ──────────────────────────
	// Every datagram: interceptor.Intercept() → jitter buffer → PTSNormalizer → WebRTC.
	if err := ingester.Listen(stop); err != nil {
		logger.Printf("ingester stopped: %v", err)
	}

	// Print final stats
	rx, dropped, fwd := interceptor.Stats()
	js := jb.Stats()
	ps := pn.Stats()
	logger.Printf("interceptor  — received=%d dropped=%d forwarded=%d", rx, dropped, fwd)
	logger.Printf("jitter buf   — in=%d released=%d evicted=%d late=%d",
		js.TotalIn, js.TotalRelease, js.TotalEvicted, js.TotalLate)
	logger.Printf("PTS norm     — normalized=%d tsIncrement=%d finalTS=%d",
		ps.TotalNormalized, ps.TSIncrement, ps.NextTS)
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

// makeStatsHandler returns a unified /api/stats handler combining
// interceptor counters, jitter buffer health, PTS normalizer state, and peer count.
func makeStatsHandler(
	pm interface{ PeerCount() int },
	interceptor interface{ Stats() (uint64, uint64, uint64) },
	jb interface{ Stats() pipeline.JitterStats },
	pn interface{ Stats() pipeline.PTSStats },
) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		rx, dropped, fwd := interceptor.Stats()
		js := jb.Stats()
		ps := pn.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"interceptor": map[string]uint64{
				"received":  rx,
				"dropped":   dropped,
				"forwarded": fwd,
			},
			"jitter_buffer": js,
			"pts_normalizer": ps,
			"peers":         pm.PeerCount(),
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

  const res = await fetch('/api/offer', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
