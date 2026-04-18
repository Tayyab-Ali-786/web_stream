// Package pipeline — Dual-Layer Jitter Buffer
//
// This file implements the two-layer anti-stutter mechanism that sits between
// the PacketInterceptor output and the WebRTC PeerManager input.
//
// WHY THIS EXISTS
// ───────────────
// Even on localhost, the Go scheduler and OS network stack can deliver packets
// in "clumps": five packets at once, then silence for 80ms, then five more.
// If those clumps go straight to the WebRTC track, the browser's decoder sees
// an irregular frame cadence and renders stutter.
//
// THE TWO LAYERS
// ──────────────
//
//  Layer 1 — Accumulator (Reordering)
//    Packets are pushed into a min-heap keyed on SequenceNumber (from PacketMeta).
//    A packet is eligible for release only after it has been held for at least
//    WindowDuration (default 60 ms). This window absorbs reordering: a packet
//    that arrives 20 ms late will still be dequeued in the correct order.
//
//  Layer 2 — Metronome (Pacing)
//    A time.Ticker fires every TickInterval (default 33.33 ms ≈ 30 FPS).
//    On every tick the buffer inspects the top of the heap and releases the
//    packet if it is both (a) the next expected sequence number and (b) past
//    its hold window. This produces a rock-steady 30 FPS heartbeat regardless
//    of how unevenly packets arrived.
//
// SEQUENCE NUMBER WRAP-AROUND
//    RTP sequence numbers are uint16 (0–65535). The buffer handles wrap-around
//    correctly using signed 16-bit difference arithmetic (RFC 3550 §A.1).
//
// USAGE
//
//	raw  := make(chan *pipeline.IngressPacket, 256)   // interceptor output
//	smooth := make(chan *pipeline.IngressPacket, 64)  // jitter buffer output → WebRTC
//
//	jb := pipeline.NewJitterBuffer(pipeline.JitterConfig{
//	    In:     raw,
//	    Out:    smooth,
//	    // optional overrides:
//	    // WindowDuration: 60 * time.Millisecond,
//	    // TickInterval:   33333 * time.Microsecond,
//	    // MaxBuffered:    120,
//	})
//	go jb.Run(stop)
package pipeline

import (
	"container/heap"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Min-heap of IngressPackets keyed on SequenceNumber
// ─────────────────────────────────────────────────────────────────────────────

// pktHeap is a min-heap of *IngressPacket sorted by SequenceNumber.
// It satisfies heap.Interface from "container/heap".
type pktHeap []*IngressPacket

func (h pktHeap) Len() int { return len(h) }

// Less compares sequence numbers using RFC 3550 §A.1 wrap-around arithmetic.
// This correctly handles the transition from 65535 → 0.
func (h pktHeap) Less(i, j int) bool {
	return seqLess(h[i].Meta.SequenceNum, h[j].Meta.SequenceNum)
}

func (h pktHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *pktHeap) Push(x interface{}) {
	*h = append(*h, x.(*IngressPacket))
}

func (h *pktHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[:n-1]
	return x
}

// seqLess returns true if sequence number a comes before b, handling wrap-around.
// Uses signed 16-bit arithmetic from RFC 3550 §A.1.
func seqLess(a, b uint16) bool {
	return int16(a-b) < 0
}

// seqEqual returns true when two sequence numbers are identical.
func seqEqual(a, b uint16) bool { return a == b }

// ─────────────────────────────────────────────────────────────────────────────
// JitterConfig
// ─────────────────────────────────────────────────────────────────────────────

// JitterConfig configures the dual-layer jitter buffer.
type JitterConfig struct {
	// In is the channel of packets coming out of the PacketInterceptor.
	In <-chan *IngressPacket

	// Out is the channel of smoothly-paced packets going to the PeerManager.
	Out chan<- *IngressPacket

	// WindowDuration is how long a packet must sit in the heap before it may
	// be released. This is the "reordering window". Default: 60 ms.
	WindowDuration time.Duration

	// TickInterval is the metronome period — how often the drain goroutine
	// attempts to release one frame. Default: 33_333 µs (≈ 30 FPS).
	TickInterval time.Duration

	// MaxBuffered is the maximum number of packets that may sit in the heap
	// simultaneously. If this limit is reached, the oldest packet is evicted
	// (best-effort recovery from severe network disruption). Default: 120.
	MaxBuffered int
}

func (c *JitterConfig) applyDefaults() {
	if c.WindowDuration == 0 {
		c.WindowDuration = 60 * time.Millisecond
	}
	if c.TickInterval == 0 {
		c.TickInterval = time.Duration(33_333 * time.Microsecond) // 30 FPS
	}
	if c.MaxBuffered == 0 {
		c.MaxBuffered = 120
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// JitterBuffer
// ─────────────────────────────────────────────────────────────────────────────

// JitterBuffer implements the two-layer anti-stutter mechanism.
//
//	Layer 1 (Accumulator): min-heap sorted by SequenceNumber.
//	Layer 2 (Metronome):   time.Ticker drains one frame per tick.
type JitterBuffer struct {
	cfg    JitterConfig
	h      pktHeap
	logger *log.Logger

	// nextSeq is the sequence number we expect to drain next.
	// Updated each time a packet is successfully released.
	nextSeq uint16

	// Atomic stats
	totalIn      uint64
	totalRelease uint64
	totalEvicted uint64
	totalLate    uint64 // packets released out-of-order due to window timeout

	initialized bool // true after the first packet anchors nextSeq
}

// NewJitterBuffer constructs a JitterBuffer. Call Run() in a goroutine.
func NewJitterBuffer(cfg JitterConfig) *JitterBuffer {
	cfg.applyDefaults()
	h := make(pktHeap, 0, cfg.MaxBuffered)
	heap.Init(&h)
	return &JitterBuffer{
		cfg:    cfg,
		h:      h,
		logger: log.New(os.Stdout, "[jitter]      ", log.LstdFlags|log.Lmicroseconds),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Run — the main event loop (blocks until stop is closed)
// ─────────────────────────────────────────────────────────────────────────────

// Run runs both layers concurrently in a single goroutine using a select loop:
//
//   - case pkt ← cfg.In  → Layer 1: push into the min-heap (Accumulator)
//   - case ← ticker.C    → Layer 2: drain candidate from heap (Metronome)
func (jb *JitterBuffer) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(jb.cfg.TickInterval)
	defer ticker.Stop()

	jb.logger.Printf("▶ jitter buffer started  window=%v  tick=%v  maxBuf=%d",
		jb.cfg.WindowDuration, jb.cfg.TickInterval, jb.cfg.MaxBuffered)

	for {
		select {
		case <-stop:
			jb.logger.Printf("■ stopped  in=%d release=%d evicted=%d late=%d heap_remaining=%d",
				atomic.LoadUint64(&jb.totalIn),
				atomic.LoadUint64(&jb.totalRelease),
				atomic.LoadUint64(&jb.totalEvicted),
				atomic.LoadUint64(&jb.totalLate),
				jb.h.Len(),
			)
			return

		// ── Layer 1 ── Accumulator ─────────────────────────────────────────
		case pkt, ok := <-jb.cfg.In:
			if !ok {
				jb.logger.Println("input channel closed")
				return
			}
			jb.accumulate(pkt)

		// ── Layer 2 ── Metronome ────────────────────────────────────────────
		case <-ticker.C:
			jb.drain()
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Layer 1 — Accumulator
// ─────────────────────────────────────────────────────────────────────────────

// accumulate pushes a packet onto the min-heap.
// If this is the very first packet it anchors nextSeq.
// If the heap is full it evicts the smallest (oldest by seq) packet.
func (jb *JitterBuffer) accumulate(pkt *IngressPacket) {
	atomic.AddUint64(&jb.totalIn, 1)

	// Anchor the expected sequence on the first ever packet.
	if !jb.initialized {
		jb.nextSeq = pkt.Meta.SequenceNum
		jb.initialized = true
		jb.logger.Printf("anchored nextSeq=%d (stream=%s)", jb.nextSeq, pkt.Meta.StreamID)
	}

	// Evict the head (earliest sequence) if the buffer is full.
	if jb.h.Len() >= jb.cfg.MaxBuffered {
		evicted := heap.Pop(&jb.h).(*IngressPacket)
		atomic.AddUint64(&jb.totalEvicted, 1)
		jb.logger.Printf("WARN heap full — evicted seq=%d", evicted.Meta.SequenceNum)
		// Advance nextSeq past the evicted packet to avoid a stall.
		jb.nextSeq = evicted.Meta.SequenceNum + 1
	}

	heap.Push(&jb.h, pkt)
}

// ─────────────────────────────────────────────────────────────────────────────
// Layer 2 — Metronome drain
// ─────────────────────────────────────────────────────────────────────────────

// drain is called on every ticker tick. It releases at most one packet per
// call, enforcing the target frame rate.
//
// Release policy (in priority order):
//  1. The heap head matches nextSeq AND has been held ≥ WindowDuration → release.
//  2. The heap head has been held ≥ 2×WindowDuration regardless of sequence → force
//     release (timeout recovery: prevents infinite stall on a lost packet).
//  3. Otherwise → hold, wait for the next tick.
func (jb *JitterBuffer) drain() {
	if jb.h.Len() == 0 {
		return
	}

	now := time.Now()
	head := jb.h[0] // peek without popping
	heldFor := now.Sub(head.ReceivedAt)

	// Case 1 — in-order release
	if seqEqual(head.Meta.SequenceNum, jb.nextSeq) && heldFor >= jb.cfg.WindowDuration {
		jb.release(heap.Pop(&jb.h).(*IngressPacket), false)
		return
	}

	// Case 2 — timeout recovery (gap in sequence; the expected packet was lost)
	forceTimeout := jb.cfg.WindowDuration * 2
	if heldFor >= forceTimeout {
		pkt := heap.Pop(&jb.h).(*IngressPacket)
		jb.logger.Printf("LATE_RELEASE seq=%d (expected=%d) held=%.0fms",
			pkt.Meta.SequenceNum, jb.nextSeq, heldFor.Seconds()*1000)
		jb.release(pkt, true)
		return
	}

	// Case 3 — hold; not yet eligible
}

// release forwards a packet to the output channel and advances nextSeq.
func (jb *JitterBuffer) release(pkt *IngressPacket, late bool) {
	if late {
		atomic.AddUint64(&jb.totalLate, 1)
	}
	atomic.AddUint64(&jb.totalRelease, 1)

	// Advance the expected sequence counter (handles uint16 wrap-around).
	jb.nextSeq = pkt.Meta.SequenceNum + 1

	select {
	case jb.cfg.Out <- pkt:
		// sent successfully
	default:
		// Output channel full (PeerManager is slow) — drop here to protect
		// the metronome: blocking would collapse the timing guarantee.
		jb.logger.Printf("WARN out channel full — sacrificing seq=%d", pkt.Meta.SequenceNum)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats snapshot
// ─────────────────────────────────────────────────────────────────────────────

// JitterStats is a point-in-time snapshot of buffer health metrics.
type JitterStats struct {
	TotalIn      uint64 `json:"total_in"`
	TotalRelease uint64 `json:"total_release"`
	TotalEvicted uint64 `json:"total_evicted"`
	TotalLate    uint64 `json:"total_late"`
	HeapDepth    int    `json:"heap_depth"`
	NextExpected uint16 `json:"next_expected_seq"`
}

// Stats returns a lock-free snapshot. HeapDepth and NextExpected are read
// without a mutex — minor races are acceptable for monitoring data.
func (jb *JitterBuffer) Stats() JitterStats {
	return JitterStats{
		TotalIn:      atomic.LoadUint64(&jb.totalIn),
		TotalRelease: atomic.LoadUint64(&jb.totalRelease),
		TotalEvicted: atomic.LoadUint64(&jb.totalEvicted),
		TotalLate:    atomic.LoadUint64(&jb.totalLate),
		HeapDepth:    jb.h.Len(),
		NextExpected: jb.nextSeq,
	}
}
