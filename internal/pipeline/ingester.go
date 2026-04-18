// Package pipeline — UDPIngester
//
// UDPIngester listens on a UDP port and passes every received datagram to
// PacketInterceptor.Intercept(). This is the exact boundary where the
// video packets "enter" the web_stream service from the simulator.
package pipeline

import (
	"log"
	"net"
	"os"
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
