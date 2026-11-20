package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"time"
)

// readOnlyConn lets crypto/tls read the client's first message without
// letting it write anything to the real connection.
type readOnlyConn struct{ r io.Reader }

func (c readOnlyConn) Read(p []byte) (int, error)     { return c.r.Read(p) }
func (readOnlyConn) Write(p []byte) (int, error)      { return len(p), nil }
func (readOnlyConn) Close() error                     { return nil }
func (readOnlyConn) LocalAddr() net.Addr              { return nil }
func (readOnlyConn) RemoteAddr() net.Addr             { return nil }
func (readOnlyConn) SetDeadline(time.Time) error      { return nil }
func (readOnlyConn) SetReadDeadline(time.Time) error  { return nil }
func (readOnlyConn) SetWriteDeadline(time.Time) error { return nil }

var errNameRead = errors.New("name read")

// readSNI reads the server name requested by the TLS client (SNI) without
// terminating TLS. It also returns the bytes already consumed, to be replayed
// to the apiserver so the TLS handshake happens directly with it.
func readSNI(c net.Conn) (string, []byte, error) {
	var consumed bytes.Buffer
	var name string
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	tls.Server(readOnlyConn{io.TeeReader(c, &consumed)}, &tls.Config{
		GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
			name = h.ServerName
			return nil, errNameRead
		},
	}).Handshake()
	if name == "" {
		return "", nil, errors.New("client did not send a cluster name (SNI)")
	}
	return name, consumed.Bytes(), nil
}

// pipe copies data both ways until either side closes.
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		io.Copy(dst, src)
		if t, ok := dst.(*net.TCPConn); ok {
			t.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	<-done
}
