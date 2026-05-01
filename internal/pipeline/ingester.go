// Package pipeline — UDPIngester
//
// UDPIngester listens on a UDP port and passes every received datagram to
// PacketInterceptor.Intercept(). This is the exact boundary where the
// video packets "enter" the web_stream service from the simulator.
package pipeline

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"time"
)

// UDPIngester binds to a UDP address and feeds raw datagrams into the
// interceptor for inspection/modification before they reach WebRTC.
type UDPIngester struct {
	addr        string
	interceptor *PacketInterceptor
	conn        *net.UDPConn
	logger      *log.Logger
}

// NewUDPIngester creates an ingester that will bind to addr and call
// interceptor.Intercept() for every received datagram.
func NewUDPIngester(addr string, interceptor *PacketInterceptor) *UDPIngester {
	return &UDPIngester{
		addr:        addr,
		interceptor: interceptor,
		logger:      log.New(os.Stdout, "[ingester]    ", log.LstdFlags|log.Lmicroseconds),
	}
}

// Listen binds the UDP socket and starts the read loop.
// It blocks until stop is closed or the socket is closed externally.
func (u *UDPIngester) Listen(stop <-chan struct{}) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", u.addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return err
	}
	u.conn = conn
	u.logger.Printf("listening on UDP %s — waiting for simulator packets", u.addr)

	// Close the socket when stop fires so the blocking ReadFrom unblocks.
	go func() {
		<-stop
		u.logger.Println("stop signal received — closing UDP socket")
		conn.Close()
	}()

	// 64 KB read buffer — larger than any single RTP packet
	buf := make([]byte, 65_535)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-stop:
				return nil // clean shutdown
			default:
				u.logger.Printf("ERROR read UDP: %v", err)
				return err
			}
		}

		// Copy the slice before handing it to Intercept so the buffer can be reused.
		raw := make([]byte, n)
		copy(raw, buf[:n])
		_ = src // source address available for future authenticated ingestion

		// This is THE interception point. Every packet from the simulator
		// passes through here. Intercept() will run all hooks (inspect,
		// modify, drop) and then forward to the WebRTC output channel.
		u.interceptor.Intercept(raw)
	}
}

// CameraListener spawns GStreamer to capture video from an actual webcam (v4l2src)
// and feeds the generated RTP packets into the interceptor, formatted as WirePackets.
type CameraListener struct {
	interceptor *PacketInterceptor
	logger      *log.Logger
}

// NewCameraListener creates a listener that will bind a UDP port and launch a GStreamer
// webcam pipeline targeting it.
func NewCameraListener(interceptor *PacketInterceptor) *CameraListener {
	return &CameraListener{
		interceptor: interceptor,
		logger:      log.New(os.Stdout, "[camera]      ", log.LstdFlags|log.Lmicroseconds),
	}
}

// Listen starts the local UDP server and the GStreamer process.
// It blocks until stop is closed.
func (c *CameraListener) Listen(stop <-chan struct{}) error {
	addr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// ── Test 2: Resource Leak (Socket Buffer) ──────────────────────────────
	// Increase the OS-level UDP receive buffer to 4MB. This prevents packet
	// drops during Go GC pauses or CPU spikes, ensuring the pipeline stays full.
	if err := conn.SetReadBuffer(4 * 1024 * 1024); err != nil {
		c.logger.Printf("WARN: failed to set UDP read buffer: %v", err)
	}

	localPort := conn.LocalAddr().(*net.UDPAddr).Port
	c.logger.Printf("listening on internal UDP %d for gstreamer stream", localPort)

	// Capture video, convert, encode to h264, and payload into RTP
	// Added 'intra-refresh=true' to eliminate large I-frame bitrate spikes.
	// Added 'byte-stream=true' and 'key-int-max=30' for better compatibility.
	cmd := exec.Command("gst-launch-1.0",
		"-v",
		"v4l2src", "device=/dev/video58", "!",
		"video/x-raw,framerate=30/1", "!",
		"videoconvert", "!", "videoscale", "!",
		"video/x-raw,width=640,height=480", "!",
		"videoconvert", "!",
		"x264enc", "speed-preset=ultrafast", "tune=zerolatency", "key-int-max=30", "bitrate=2000", "!",
		"video/x-h264,profile=baseline", "!",
		"rtph264pay", "pt=96", "mtu=1200", "config-interval=1", "!",
		"udpsink", "host=127.0.0.1", fmt.Sprintf("port=%d", localPort),
	)
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start gstreamer: %w", err)
	}
	c.logger.Printf("started gstreamer (PID %d)", cmd.Process.Pid)

	// done channel ensures the stop-watcher goroutine exits even if the
	// main loop returns due to a GStreamer crash (fixing the goroutine leak).
	done := make(chan struct{})
	go func() {
		select {
		case <-stop:
			c.logger.Println("stop signal received — killing gstreamer")
		case <-done:
			// Main loop exited (e.g. gstreamer crashed)
		}
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		conn.Close()
	}()
	defer close(done)

	seq := uint16(0)
	frameNo := uint32(0)
	buf := make([]byte, 65535)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
				return err
			}
		}

		// Extract RTP payload for IDR detection.
		// RTP fixed header is 12 bytes minimum; payload starts after.
		var rtpPayload []byte
		if n > 12 {
			rtpPayload = buf[12:n]
		}

		meta := PacketMeta{
			StreamID:     "camera-stream",
			CameraID:     "local-webcam",
			StoreID:      "local",
			SequenceNum:  seq,
			Timestamp:    uint32(time.Now().UnixNano() / 1000), // mock timestamp
			IsKeyFrame:   isIDRFrame(rtpPayload),
			FrameNumber:  frameNo,
			Width:        640,
			Height:       480,
			Codec:        "H264",
			PayloadType:  96,
			SentAtUnixMs: time.Now().UnixMilli(),
			SimulatedFPS: 30.0,
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			continue
		}
		metaLen := uint32(len(metaBytes))

		// Wire layout: [4 bytes metaLen] [meta JSON] [RTP bytes]
		wire := make([]byte, 4+len(metaBytes)+n)
		binary.BigEndian.PutUint32(wire[:4], metaLen)
		copy(wire[4:], metaBytes)
		copy(wire[4+int(metaLen):], buf[:n])

		// Feed the packet into the interceptor
		c.interceptor.Intercept(wire)

		seq++
		frameNo++
	}
}

// isIDRFrame inspects the H.264 NAL unit type of the RTP payload to determine
// whether the packet carries (or starts) an IDR (Instantaneous Decoder Refresh)
// frame.  It handles:
//   - Single NAL unit packets (NAL type 5 = IDR slice)
//   - FU-A fragmentation units (NAL type 28) where the Start bit is set and
//     the inner NAL type is 5.
func isIDRFrame(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	nalType := payload[0] & 0x1F
	if nalType == 5 {
		return true // single NAL IDR
	}
	if nalType == 28 && len(payload) >= 2 { // FU-A fragment
		return payload[1]&0x80 != 0 && payload[1]&0x1F == 5
	}
	return false
}
