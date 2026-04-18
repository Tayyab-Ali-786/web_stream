// Package pipeline provides the packet interception layer that sits between the
// UDP ingestion point and the WebRTC track sender.
//
// Architecture:
//
//	[Simulator UDP] ──► [UDPIngester.Listen]
//	                          │
//	                          ▼
//	              [PacketInterceptor.Intercept]  ◄── hook point
//	                          │
//	                    ┌─────┴──────┐
//	                    │  inspect   │  log / metrics / modify
//	                    └─────┬──────┘
//	                          │
//	                    ┌─────┴──────┐
//	                    │  filter    │  drop bad packets
//	                    └─────┬──────┘
//	                          │
//	                    ┌─────┴──────┐
//	                    │  forward   │  push to WebRTC channel
//	                    └────────────┘
//
// Every raw byte slice received from the network passes through Intercept()
// before it is handed to the WebRTC subsystem. Hook functions registered via
// AddHook() run in registration order and may mutate or drop a packet.
package pipeline

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

// ─────────────────────────────────────────────────────────────────────────────
// Wire format (must mirror the simulator)
// ─────────────────────────────────────────────────────────────────────────────

// PacketMeta mirrors the simulator's per-packet metadata structure.
// It is decoded from the front of every datagram.
type PacketMeta struct {
	StreamID     string  `json:"stream_id"`
	CameraID     string  `json:"camera_id"`
	StoreID      string  `json:"store_id"`
	SequenceNum  uint16  `json:"seq"`
	Timestamp    uint32  `json:"ts"`
	IsKeyFrame   bool    `json:"key_frame"`
	FrameNumber  uint32  `json:"frame_no"`
	Width        int     `json:"width"`
	Height       int     `json:"height"`
	Codec        string  `json:"codec"`
	PayloadType  uint8   `json:"pt"`
	SentAtUnixMs int64   `json:"sent_at_ms"`
	SimulatedFPS float64 `json:"fps"`
}

// IngressPacket is the decoded, fully-parsed form of a raw datagram.
// After Intercept() returns it, the WebRTC layer consumes only the RTP field.
type IngressPacket struct {
	Meta       PacketMeta
	RTP        *rtp.Packet
	ReceivedAt time.Time
	// LatencyMs is the one-way delay from the simulator clock to now.
	LatencyMs float64
	// Dropped is set to true by a hook that wants to discard this packet.
	Dropped bool
}

// ParseWire decodes a raw UDP datagram into an IngressPacket.
//
// Wire layout:
//
//	[4 bytes big-endian uint32: metaLen] [metaLen bytes JSON] [RTP bytes]
func ParseWire(raw []byte) (*IngressPacket, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("datagram too short (%d bytes)", len(raw))
	}
	metaLen := binary.BigEndian.Uint32(raw[:4])
	if uint32(len(raw)) < 4+metaLen {
		return nil, fmt.Errorf("datagram truncated: want %d meta bytes, have %d",
			metaLen, len(raw)-4)
	}

	metaBytes := raw[4 : 4+metaLen]
	rtpBytes := raw[4+metaLen:]

	var meta PacketMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("parse meta JSON: %w", err)
	}

	rtpPkt := &rtp.Packet{}
	if err := rtpPkt.Unmarshal(rtpBytes); err != nil {
		return nil, fmt.Errorf("parse RTP: %w", err)
	}

	now := time.Now()
	sentAt := time.UnixMilli(meta.SentAtUnixMs)
	latency := float64(now.Sub(sentAt).Milliseconds())

	return &IngressPacket{
		Meta:       meta,
		RTP:        rtpPkt,
		ReceivedAt: now,
		LatencyMs:  latency,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Hook system
// ─────────────────────────────────────────────────────────────────────────────

// HookFn is called for every packet entering the pipeline.
// Implementations may:
//   - Inspect pkt.Meta and pkt.RTP for logging / metrics
//   - Modify pkt.RTP.Payload or pkt.RTP.Header fields
//   - Set pkt.Dropped = true to prevent the packet from reaching WebRTC
//
// Hooks MUST NOT block for more than a few microseconds; long work should be
// dispatched to a goroutine.
type HookFn func(pkt *IngressPacket)

// namedHook pairs an identifier with its function so hooks can be removed.
type namedHook struct {
	name string
	fn   HookFn
}

// ─────────────────────────────────────────────────────────────────────────────
// PacketInterceptor
// ─────────────────────────────────────────────────────────────────────────────

// PacketInterceptor is the central hook point between UDP ingestion and WebRTC.
//
//	interceptor := pipeline.NewPacketInterceptor(outCh)
//	interceptor.AddHook("logger", myLoggingHook)
//	interceptor.AddHook("dropper", myDropHook)
//	// … then pass raw bytes via:
//	interceptor.Intercept(rawBytes)
type PacketInterceptor struct {
	mu     sync.RWMutex
	hooks  []namedHook
	out    chan<- *IngressPacket // forwarded (non-dropped) packets go here
	logger *log.Logger

	// Atomic counters — safe to read from any goroutine
	totalReceived uint64
	totalDropped  uint64
	totalForwarded uint64
}

// NewPacketInterceptor creates an interceptor that forwards packets to out.
// The caller is responsible for consuming from out in a separate goroutine.
func NewPacketInterceptor(out chan<- *IngressPacket) *PacketInterceptor {
	pi := &PacketInterceptor{
		out:    out,
		logger: log.New(os.Stdout, "[interceptor] ", log.LstdFlags|log.Lmicroseconds),
	}
	// Register the built-in inspection hook as the very first hook.
	pi.AddHook("built-in:inspect", pi.builtInInspectHook)
	return pi
}

// AddHook appends a named hook to the pipeline. Hooks run in add order.
func (pi *PacketInterceptor) AddHook(name string, fn HookFn) {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	pi.hooks = append(pi.hooks, namedHook{name: name, fn: fn})
	pi.logger.Printf("hook registered: %q (total hooks: %d)", name, len(pi.hooks))
}

// RemoveHook removes the first hook registered under name.
func (pi *PacketInterceptor) RemoveHook(name string) bool {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	for i, h := range pi.hooks {
		if h.name == name {
			pi.hooks = append(pi.hooks[:i], pi.hooks[i+1:]...)
			pi.logger.Printf("hook removed: %q", name)
			return true
		}
	}
	return false
}

// Intercept is the single entry point for all raw datagrams.
// It parses the wire format, runs all registered hooks in order, and —
// unless a hook set pkt.Dropped — sends the packet to the WebRTC output channel.
//
// This is the "hook-in" described in the task: every packet from the simulator
// must pass through here before reaching the WebRTC logic.
func (pi *PacketInterceptor) Intercept(raw []byte) {
	atomic.AddUint64(&pi.totalReceived, 1)

	pkt, err := ParseWire(raw)
	if err != nil {
		pi.logger.Printf("ERROR parse wire: %v (dropping %d bytes)", err, len(raw))
		atomic.AddUint64(&pi.totalDropped, 1)
		return
	}

	// Run hooks under read-lock so hooks can be added concurrently
	pi.mu.RLock()
	hooks := make([]namedHook, len(pi.hooks))
	copy(hooks, pi.hooks)
	pi.mu.RUnlock()

	for _, h := range hooks {
		h.fn(pkt)
		if pkt.Dropped {
			pi.logger.Printf("packet #%d dropped by hook %q", pkt.Meta.SequenceNum, h.name)
			atomic.AddUint64(&pi.totalDropped, 1)
			return
		}
	}

	// Forward to WebRTC layer
	select {
	case pi.out <- pkt:
		atomic.AddUint64(&pi.totalForwarded, 1)
	default:
		// Output channel full — drop rather than block the ingestion goroutine
		pi.logger.Printf("WARN output channel full — dropping packet #%d", pkt.Meta.SequenceNum)
		atomic.AddUint64(&pi.totalDropped, 1)
	}
}

// Stats returns a snapshot of packet counters.
func (pi *PacketInterceptor) Stats() (received, dropped, forwarded uint64) {
	return atomic.LoadUint64(&pi.totalReceived),
		atomic.LoadUint64(&pi.totalDropped),
		atomic.LoadUint64(&pi.totalForwarded)
}

// ─────────────────────────────────────────────────────────────────────────────
// Built-in inspection hook
// ─────────────────────────────────────────────────────────────────────────────

// builtInInspectHook is the default first hook. It logs every packet header and
// emits a visible marker for key-frames. It never drops packets.
func (pi *PacketInterceptor) builtInInspectHook(pkt *IngressPacket) {
	keyTag := ""
	if pkt.Meta.IsKeyFrame {
		keyTag = " *** KEY-FRAME ***"
	}

	received, _, forwarded := pi.Stats()
	// Log every 30th packet to avoid flooding, but always log key-frames
	if pkt.Meta.IsKeyFrame || received%30 == 1 {
		pi.logger.Printf(
			"PKT stream=%s cam=%s seq=%d ts=%d frame#=%d size=%d lat=%.1fms%s  (rx=%d fwd=%d)",
			pkt.Meta.StreamID,
			pkt.Meta.CameraID,
			pkt.Meta.SequenceNum,
			pkt.Meta.Timestamp,
			pkt.Meta.FrameNumber,
			len(pkt.RTP.Payload),
			pkt.LatencyMs,
			keyTag,
			received,
			forwarded,
		)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Ready-to-use hook builders (callers may add these via AddHook)
// ─────────────────────────────────────────────────────────────────────────────

// DropNonKeyFramesHook returns a HookFn that drops every non-key-frame packet.
// Useful for testing that the interceptor can selectively filter the pipeline.
func DropNonKeyFramesHook() HookFn {
	return func(pkt *IngressPacket) {
		if !pkt.Meta.IsKeyFrame {
			pkt.Dropped = true
		}
	}
}

// LatencyAlertHook returns a HookFn that logs (but does not drop) packets whose
// one-way latency exceeds thresholdMs.
func LatencyAlertHook(thresholdMs float64, logger *log.Logger) HookFn {
	return func(pkt *IngressPacket) {
		if pkt.LatencyMs > thresholdMs {
			logger.Printf("LATENCY ALERT seq=%d lat=%.1fms (threshold=%.0fms)",
				pkt.Meta.SequenceNum, pkt.LatencyMs, thresholdMs)
		}
	}
}

// TimestampRewriteHook returns a HookFn that performs "Linear Timestamp
// Normalization" on every RTP packet passing through the interceptor.
//
// # The Problem with the Old Approach
//
// The original implementation used time.Since(startWall) to derive a timestamp.
// That has two critical flaws:
//
//  1. Wall-clock drift: if two packets arrive 1ms apart, their timestamps
//     differ by only 90 units (at 90kHz). The browser expects them to differ
//     by exactly clockRate/FPS (e.g. 3000 for 30FPS). This causes the browser
//     to mis-schedule frames → micro-stutter.
//
//  2. Arrival-time stamping: the hook runs before the JitterBuffer, so the
//     timestamp reflects when the packet hit the server, not when it was
//     transmitted to the browser. The delta between those two moments is
//     exactly the jitter we are trying to hide.
//
// # The Fix — Linear Timestamp Normalization
//
// For every packet emitted, we assign:
//
//	NewTimestamp = PreviousTimestamp + (ClockRate / TargetFPS)
//
// For a 90 kHz clock at 30 FPS: each packet advances by exactly 3000 units.
// The first packet uses a cryptographically-random RFC 3550 §5.1 seed so that
// session timestamps can't be trivially guessed.
//
// NOTE: When the full PTSNormalizer stage is active (Step 3), this hook's
// output will be overwritten by the normalizer. This hook is upgraded here
// so that any direct caller (e.g. unit tests, lightweight pipelines without
// a JitterBuffer) also gets correct linear behaviour.
//
// Parameters:
//
//	clockRate – RTP clock frequency in Hz (standard H.264 = 90,000)
//	targetFPS – target frame rate; determines the per-packet increment
func TimestampRewriteHook(clockRate uint32, targetFPS float64) HookFn {
	if targetFPS <= 0 {
		targetFPS = 30.0
	}
	increment := uint32(float64(clockRate) / targetFPS)

	// RFC 3550 §5.1: seed with a random offset so the starting value is
	// not predictable. Fall back gracefully if crypto/rand is unavailable.
	seed := cryptoRandUint32Hook()

	var (
		mu          sync.Mutex
		nextTS      = seed
		initialized = false
	)

	return func(pkt *IngressPacket) {
		mu.Lock()
		defer mu.Unlock()

		if !initialized {
			// Anchor: first packet gets the random seed directly.
			pkt.RTP.Header.Timestamp = nextTS
			nextTS += increment
			initialized = true
			return
		}

		// Every subsequent packet: strict linear increment.
		// uint32 overflow is intentional — it wraps correctly per RFC 3550.
		pkt.RTP.Header.Timestamp = nextTS
		nextTS += increment
	}
}

// cryptoRandUint32Hook returns a cryptographically-random uint32 for use as
// an RTP timestamp seed (RFC 3550 §5.1). Kept package-private so pts.go and
// the hook can both use the same approach without duplicating the logic.
func cryptoRandUint32Hook() uint32 {
	buf := make([]byte, 4)
	if _, err := cryptorand.Read(buf); err != nil {
		// Non-fatal fallback — zero is a valid (if weak) seed.
		return 0xDEAD_C0DE
	}
	return binary.BigEndian.Uint32(buf)
}

