# web_stream — Low-Latency WebRTC Streaming Server (Go)

A real-time video streaming server written in **Go** on top of **[Pion WebRTC](https://github.com/pion/webrtc)**.
It captures H.264 video with **GStreamer**, receives it over **UDP/RTP**, paces it frame by frame, and
broadcasts it to any number of browser viewers over WebRTC — with **sub-33 ms pipeline latency**
(down from ~600 ms in the naive packet-paced design).

Browser client: **[Tayyab-Ali-786/web](https://github.com/Tayyab-Ali-786/web)** (Next.js live stream viewer).

---

## Highlights

- **~600 ms → under 33 ms latency** with a frame-aware pacing stage that bursts whole H.264 Access Units
  instead of trickling single RTP packets
- **Go concurrency pipeline** — goroutines and buffered channels connect each stage; graceful shutdown on `SIGINT`/`SIGTERM`
- **H.264 over RTP done properly** — FU-A reassembly, end-of-frame detection via the RTP marker bit (RFC 6184),
  IDR keyframe detection, RFC 3550-compliant linear timestamp normalization
- **Pluggable packet-interceptor** — ordered hook chain to inspect, modify or drop packets (built-in latency-alert hook)
- **Fan-out to many viewers** — one shared Pion video track, one `PeerConnection` per browser
- **Observability** — `/api/stats` metrics endpoint and `pprof` profiling on `:6060`

## Architecture

```
GStreamer (v4l2src → x264enc zerolatency → rtph264pay)
        │  UDP / RTP
        ▼
CameraListener ──► PacketInterceptor ──► FrameMetronome ──► PeerManager ──► browsers (WebRTC)
                   (hook chain)          (AU assembly,       (Pion track,
                                          33 ms tick,         SDP signaling)
                                          PTS normalization)
```

| Stage | File | What it does |
|---|---|---|
| Ingest | `internal/pipeline/ingester.go` | Spawns GStreamer, reads RTP datagrams from UDP, tags IDR frames |
| Intercept | `internal/pipeline/interceptor.go` | Runs registered hooks in order; can log, mutate or drop packets |
| Pace | `internal/pipeline/frame_metronome.go` | Groups packets into complete Access Units, releases one frame per tick |
| Reorder (legacy) | `internal/pipeline/jitter.go` | Min-heap jitter buffer keyed on sequence number |
| Timestamps | `internal/pipeline/pts.go` | Linear RTP timestamp rewrite: `ts += clockRate / fps` (3000 @ 90 kHz, 30 FPS) |
| WebRTC | `internal/webrtc/peer.go` | Pion `PeerConnection` per viewer, SDP offer/answer, shared track |

### Why the latency dropped

A 1080p H.264 frame is split into 20–50 FU-A RTP packets. Releasing **one packet per 33 ms tick** made each frame
take hundreds of milliseconds to reach the decoder, and the browser kept repainting stale frames.
The **Frame Metronome** waits for the full Access Unit (marker bit), then sends every packet of that frame
in a single burst on the next tick — so latency is bounded by one frame interval.

## Getting started

**Requirements:** Go 1.25+, GStreamer 1.x with `x264enc` (`gst-plugins-ugly`), a V4L2 webcam (Linux).

```bash
git clone https://github.com/Tayyab-Ali-786/web_stream.git
cd web_stream
go run . -http :8080 -fps 30
```

> The capture device is set in `internal/pipeline/ingester.go` (`device=/dev/video58`). Change it to your camera,
> e.g. `/dev/video0`.

Then open the built-in viewer at `http://localhost:8080`, or run the Next.js client from the
[web](https://github.com/Tayyab-Ali-786/web) repo.

### Flags

| Flag | Default | Description |
|---|---|---|
| `-http` | `:8080` | HTTP signaling server address |
| `-fps` | `30` | Target output frame rate |
| `-buf` | `256` | Channel buffer depth between stages |

### HTTP API

| Method | Route | Description |
|---|---|---|
| `POST` | `/api/auth/login` | Returns a prototype bearer token |
| `POST` | `/api/offer` | Takes an SDP offer, returns the SDP answer (ICE gathered server-side) |
| `GET` | `/api/stats` | Interceptor, metronome and peer metrics |
| `GET` | `/debug/pprof` | Go profiler (port `6060`) |

## Tech stack

Go · Pion WebRTC v4 · Pion RTP · GStreamer · x264 · H.264 · RTP / RTCP · SDP / ICE / STUN · pprof

## Status

Prototype. Auth uses a fixed mock token — swap in real auth (e.g. JWT) before exposing it publicly.

---

Built by **[Tayyab Ali Rashid](https://www.linkedin.com/in/tayyab-ali-rashid-a471a7309/)**
