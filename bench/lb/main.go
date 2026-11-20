// lb is a minimal TCP load balancer for the tests: every connection arriving
// on one of the ports goes, in turn, to the same port of one of the backends.
// A backend that does not answer is skipped. In production use a real one.
//
//	lb -listen 10.0.0.1 -ports 6443,30000 -backends 127.0.0.1,127.0.0.2
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	listen := flag.String("listen", "", "address to listen on")
	ports := flag.String("ports", "", "comma-separated ports")
	backends := flag.String("backends", "", "comma-separated backend addresses")
	flag.Parse()
	hosts := strings.Split(*backends, ",")
	var turn atomic.Uint64
	for _, p := range strings.Split(*ports, ",") {
		l, err := net.Listen("tcp", net.JoinHostPort(*listen, p))
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go func() {
					defer c.Close()
					for range hosts {
						h := hosts[turn.Add(1)%uint64(len(hosts))]
						b, err := net.DialTimeout("tcp", net.JoinHostPort(h, p), 2*time.Second)
						if err != nil {
							continue
						}
						defer b.Close()
						go func() { io.Copy(b, c); b.(*net.TCPConn).CloseWrite() }()
						io.Copy(c, b)
						return
					}
				}()
			}
		}()
	}
	log.Print("lb ready")
	select {}
}
