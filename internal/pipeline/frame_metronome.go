// Package pipeline — Frame-Aware Metronome
//
// FrameMetronome replaces the old packet-level ticker in the JitterBuffer's
// Layer 2 drain step.  Instead of releasing ONE RTP packet per 33ms tick it:
//
//  1. Accumulates all RTP packets that belong to a single H.264 Access Unit (AU)
//     by watching for pkt.RTP.Header.Marker == true  (RFC 6184 §5.1, end-of-AU).
//  2. Waits for the 33.3ms heartbeat ONCE per complete frame.
//  3. Bursts every packet in the AU to track.WriteRTP in a tight loop —
//     zero artificial inter-packet delay, all sharing the same RTP Timestamp.
//
// Why the old packet-ticker caused 600ms latency
// ───────────────────────────────────────────────
// A 1080p H.264 frame is typically fragmented into 20-50 FU-A RTP packets
// (each ≤1500 bytes, the Ethernet MTU).  With a 33ms ticker each individual
// packet waited up to 33ms before being forwarded:
//
//   50 packets × 33ms = 1650ms worst-case; typical ~600–800ms pipeline latency.
//
// The browser's H.264 decoder cannot render until it holds the COMPLETE AU.
// While the packets trickled in one per tick the decoder kept accumulating
// incomplete NAL units, causing it to output the previous (stale) frame
// repeatedly — the visual "smearing" / ghost-frame effect.
//
// How the Frame-Aware Metronome fixes it
// ────────────────────────────────────────
//  • The ticker fires once per frame interval (33.3ms @ 30 FPS).
//  • On tick, the entire queued frame (all 20-50 packets) is burst out
//    in microseconds, not milliseconds.
//  • All packets carry the same RTP Timestamp (locked at accumulation time),
//    so the browser decoder sees a coherent AU and can render immediately.
//  • Round-trip latency drops from ~600ms → <33ms (one frame interval).
//
// Drop-in integration
// ───────────────────
// Wire FrameMetronome between the PacketInterceptor output and the PeerManager:
//
//	rawCh   := make(chan *pipeline.IngressPacket, 256)   // interceptor → here
//	webrtcCh := make(chan *pipeline.IngressPacket, 256)  // here → PeerManager
//
//	fm := pipeline.NewFrameMetronome(pipeline.FrameMetronomeConfig{
//	    In:        rawCh,
//	    Out:       webrtcCh,
//	    TargetFPS: 30.0,
//	    ClockRate: 90_000,
//	})
//	go fm.Run(stop)
//
// Or register it as a drop-in HookFn via the PacketInterceptor — see
// NewFrameMetronomeHook() at the bottom of this file.
package pipeline

import (
	"crypto/rand"
	"encoding/binary"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// FrameMetronomeConfig
// ─────────────────────────────────────────────────────────────────────────────

// FrameMetronomeConfig configures the frame-aware pacing stage.
type FrameMetronomeConfig struct {
	// In is the channel of RTP packets coming from the PacketInterceptor
	// (or the JitterBuffer's reorder heap output).
	In <-chan *IngressPacket

	// Out is the channel going to the PeerManager (track.WriteRTP consumer).
	Out chan<- *IngressPacket

	// TargetFPS is the target output frame rate.
	// Determines the ticker interval: tickInterval = 1s / TargetFPS.
	// Default: 30.0
	TargetFPS float64

	// ClockRate is the RTP clock frequency in Hz.
	// Standard H.264 per RFC 6184 §8.3: 90,000 Hz.
	// Default: 90_000
	ClockRate uint32

	// MaxFramePackets is the maximum number of RTP packets we buffer before
	// treating the frame as complete even without a Marker bit (safety valve
	// for malformed encoders).  Default: 128.
	MaxFramePackets int

	// FrameDropTimeout is how long we wait for a Marker-terminated frame
	// before force-flushing whatever we have (handles encoder stalls).
	// Default: 5 × tickInterval.
	FrameDropTimeout time.Duration
}

func (c *FrameMetronomeConfig) applyDefaults() {
	if c.TargetFPS <= 0 {
		c.TargetFPS = 30.0
	}
	if c.ClockRate == 0 {
		c.ClockRate = 90_000
	}
	if c.MaxFramePackets <= 0 {
		c.MaxFramePackets = 128
	}
	if c.FrameDropTimeout == 0 {
		tickInterval := time.Duration(float64(time.Second) / c.TargetFPS)
		c.FrameDropTimeout = 5 * tickInterval
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FrameMetronome
// ─────────────────────────────────────────────────────────────────────────────

// FrameMetronome accumulates RTP packets per H.264 Access Unit and bursts
// each complete frame on the metronome tick.
type FrameMetronome struct {
	cfg         FrameMetronomeConfig
	tsIncrement uint32 // 90000 / FPS, e.g. 3000 for 30fps
	logger      *log.Logger

	// ── Test 3: Timestamp Normalization ────────────────────────────────────
	nextTS      uint32 // assigned to the next outgoing AU
	initialized bool   // seeded on first frame

	// readyCh carries complete frames (slices of IngressPacket) from the
	// accumulator goroutine to the metronome goroutine.
	readyCh chan []*IngressPacket

	// Atomic stats
	totalFramesIn      uint64 // frames accumulated
	totalFramesBurst   uint64 // frames burst to Out
	totalPacketsOut    uint64 // individual RTP packets written to Out
	totalForceFlushes  uint64 // frames flushed due to timeout/cap (malformed)
	totalTimestampFix  uint64 // packets whose timestamp was unified in the burst
}

// NewFrameMetronome constructs a FrameMetronome.
func NewFrameMetronome(cfg FrameMetronomeConfig) *FrameMetronome {
	cfg.applyDefaults()
	inc := uint32(float64(cfg.ClockRate) / cfg.TargetFPS)
	return &FrameMetronome{
		cfg:         cfg,
		tsIncrement: inc,
		readyCh:     make(chan []*IngressPacket, 15),
		logger:      log.New(os.Stdout, "[frame-metro] ", log.LstdFlags|log.Lmicroseconds),
	}
}

// Run starts the accumulator and the metronome goroutines.
func (fm *FrameMetronome) Run(stop <-chan struct{}) {
	tickInterval := time.Duration(float64(time.Second) / fm.cfg.TargetFPS)
	fm.logger.Printf(
		"▶ frame metronome started  fps=%.1f  tick=%v  tsIncrement=%d  maxPkt=%d  dropTimeout=%v",
		fm.cfg.TargetFPS, tickInterval, fm.tsIncrement,
		fm.cfg.MaxFramePackets, fm.cfg.FrameDropTimeout,
	)

	accDone := make(chan struct{})
	go func() {
		defer close(accDone)
		fm.runAccumulator(stop)
	}()

	fm.runMetronome(stop, tickInterval)

	<-accDone

	fm.logger.Printf(
		"■ stopped  framesIn=%d burst=%d pktsOut=%d forceFlush=%d tsFixed=%d",
		atomic.LoadUint64(&fm.totalFramesIn),
		atomic.LoadUint64(&fm.totalFramesBurst),
		atomic.LoadUint64(&fm.totalPacketsOut),
		atomic.LoadUint64(&fm.totalForceFlushes),
		atomic.LoadUint64(&fm.totalTimestampFix),
	)
}

// runAccumulator reads from cfg.In and groups packets into complete H.264 AUs.
func (fm *FrameMetronome) runAccumulator(stop <-chan struct{}) {
	frameBuffer := make([]*IngressPacket, 0, fm.cfg.MaxFramePackets)
	timeout := time.NewTimer(fm.cfg.FrameDropTimeout)
	timeout.Stop()

	defer func() {
		timeout.Stop()
		if len(frameBuffer) > 0 {
			fm.pushFrame(frameBuffer)
		}
		close(fm.readyCh)
	}()

	for {
		select {
		case <-stop:
			return

		case pkt, ok := <-fm.cfg.In:
			if !ok {
				return
			}

			if len(frameBuffer) == 0 {
				timeout.Reset(fm.cfg.FrameDropTimeout)
			}

			frameBuffer = append(frameBuffer, pkt)

			if pkt.RTP.Header.Marker {
				timeout.Stop()
				fm.pushFrame(frameBuffer)
				frameBuffer = frameBuffer[:0]
				continue
			}

			if len(frameBuffer) >= fm.cfg.MaxFramePackets {
				timeout.Stop()
				atomic.AddUint64(&fm.totalForceFlushes, 1)
				fm.pushFrame(frameBuffer)
				frameBuffer = frameBuffer[:0]
			}

		case <-timeout.C:
			if len(frameBuffer) > 0 {
				atomic.AddUint64(&fm.totalForceFlushes, 1)
				fm.pushFrame(frameBuffer)
				frameBuffer = frameBuffer[:0]
			}
		}
	}
}

// pushFrame applies linear timestamp normalization and enqueues a complete AU.
//
// Linear Timestamp Normalization (RFC 3550 §5.1):
//   - On the first frame, seed nextTS with a cryptographically-random value.
//   - On every subsequent frame, assign nextTS = prevTS + tsIncrement.
//   - ALL packets in the AU receive the SAME normalized timestamp so the
//     browser decoder sees a coherent Access Unit and schedules it correctly.
//
// This hides any jitter from the upstream source (v4l2src, GStreamer PTS drift,
// OS scheduling) and produces a perfectly linear 90kHz clock for the browser.
func (fm *FrameMetronome) pushFrame(frame []*IngressPacket) {
	if len(frame) == 0 {
		return
	}

	// ── Linear Timestamp Normalization ─────────────────────────────────────
	if !fm.initialized {
		fm.nextTS = fm.cryptoRandUint32()
		fm.initialized = true
		fm.logger.Printf("timestamp seed (RFC 3550 random): %d", fm.nextTS)
	}

	normalizedTS := fm.nextTS
	for _, pkt := range frame {
		if pkt.RTP.Header.Timestamp != normalizedTS {
			// Track how many packets had their timestamp corrected.
			atomic.AddUint64(&fm.totalTimestampFix, 1)
		}
		pkt.RTP.Header.Timestamp = normalizedTS
	}
	// Advance the clock for the next frame regardless of whether this frame
	// was Marker-terminated or force-flushed. This keeps the sequence linear.
	fm.nextTS += fm.tsIncrement

	out := make([]*IngressPacket, len(frame))
	copy(out, frame)

	atomic.AddUint64(&fm.totalFramesIn, 1)

	select {
	case fm.readyCh <- out:
	default:
		// Metronome goroutine is behind; evict the oldest queued frame to
		// make room rather than dropping the freshly-accumulated one.
		select {
		case dropped := <-fm.readyCh:
			fm.logger.Printf("WARN readyCh full — evicting stale frame (%d pkts)", len(dropped))
		default:
		}
		select {
		case fm.readyCh <- out:
		default:
			// Still full — discard rather than block the accumulator.
			fm.logger.Printf("WARN readyCh still full — discarding frame (%d pkts)", len(out))
		}
	}
}

// runMetronome is the heartbeat loop.
func (fm *FrameMetronome) runMetronome(stop <-chan struct{}, tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return

		case <-ticker.C:
			select {
			case frame, ok := <-fm.readyCh:
				if !ok {
					return
				}
				// ── Pacing Budget ──────────────────────────────────────────
				fm.burstFrame(frame, tickInterval)
			default:
			}
		}
	}
}

// burstFrame writes every packet in a complete AU to cfg.Out as a tight burst,
// then optionally sleeps any remaining budget to stay in sync with the ticker.
//
// Pacing design — budget floor, NOT per-packet spread:
//
//  • ALL packets are written to Out in a tight loop with zero inter-packet delay.
//    The browser's H.264 decoder needs the COMPLETE Access Unit; drip-feeding
//    packets across the 33ms window would re-introduce the original 600ms
//    fragmentation latency we designed against.
//
//  • After the burst, if time remains within 90% of the tick budget, we sleep
//    the residual.  This prevents the metronome from "spinning" on a fast machine
//    and consuming CPU without doing useful work.  It is a floor, not a ceiling:
//    a large frame that takes >30ms to write still goes out without interruption.
func (fm *FrameMetronome) burstFrame(frame []*IngressPacket, budget time.Duration) {
	if len(frame) == 0 {
		return
	}
	atomic.AddUint64(&fm.totalFramesBurst, 1)

	burstStart := time.Now()

	// ── Tight burst: all packets out with zero artificial delay ────────────
	for _, pkt := range frame {
		select {
		case fm.cfg.Out <- pkt:
			atomic.AddUint64(&fm.totalPacketsOut, 1)
		default:
			// Out channel full (PeerManager lagging) — drop rather than block.
			fm.logger.Printf(
				"WARN out full — dropping pkt seq=%d ts=%d marker=%v",
				pkt.Meta.SequenceNum, pkt.RTP.Header.Timestamp, pkt.RTP.Header.Marker,
			)
		}
	}

	// ── Budget floor: sleep remaining slack (≤90% of tick interval) ────────
	// This is not pacing; it is simply preventing idle spin between ticks.
	elapsed := time.Since(burstStart)
	allowance := budget * 9 / 10
	if elapsed < allowance {
		time.Sleep(allowance - elapsed)
	}
}

// cryptoRandUint32 returns a cryptographically-random uint32.
func (fm *FrameMetronome) cryptoRandUint32() uint32 {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return 0xDEADBEEF
	}
	return binary.BigEndian.Uint32(buf)
}

// FrameMetronomeStats is a snapshot of metronome health metrics.
type FrameMetronomeStats struct {
	FramesAccumulated uint64 `json:"frames_accumulated"`
	FramesBurst       uint64 `json:"frames_burst"`
	PacketsOut        uint64 `json:"packets_out"`
	ForceFlushes      uint64 `json:"force_flushes"`
	TimestampFixes    uint64 `json:"timestamp_fixes"`
	ReadyQueueDepth   int    `json:"ready_queue_depth"`
}

// Stats returns a snapshot of the metronome's counters.
func (fm *FrameMetronome) Stats() FrameMetronomeStats {
	return FrameMetronomeStats{
		FramesAccumulated: atomic.LoadUint64(&fm.totalFramesIn),
		FramesBurst:       atomic.LoadUint64(&fm.totalFramesBurst),
		PacketsOut:        atomic.LoadUint64(&fm.totalPacketsOut),
		ForceFlushes:      atomic.LoadUint64(&fm.totalForceFlushes),
		TimestampFixes:    atomic.LoadUint64(&fm.totalTimestampFix),
		ReadyQueueDepth:   len(fm.readyCh),
	}
}

// NewFrameMetronomeHook wraps a FrameMetronome as a pipeline HookFn.
func NewFrameMetronomeHook(hookIn chan<- *IngressPacket) HookFn {
	return func(pkt *IngressPacket) {
		select {
		case hookIn <- pkt:
		default:
		}
		pkt.Dropped = true
	}
}
