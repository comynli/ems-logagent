package send

import (
	"github.com/lixm/ems/common"
	tomb "gopkg.in/tomb.v2"
	"log"
	"math/rand"
	"net"
)

type Sender struct {
	tomb.Tomb
	addrs []string
	queue chan common.TraceItem
}

func (s *Sender) send() error {
	for {
		select {
		case <-s.Dying():
			return nil
		case ti := <-s.queue:
			buf, err := ti.Encode()
			if err != nil {
				log.Println("treace item can not encode")
				continue
			}
			addr := s.addrs[rand.Intn(len(s.addrs))]
			conn, err := net.Dial("udp", addr)
			if err != nil {
				s.queue <- ti
				log.Printf("connect to %s fail %s", addr, err)
			}
			conn.Write(buf)
			conn.Close()
		}
	}
}

func (s *Sender) Stop() error {
	s.Kill(nil)
	return s.Wait()
}

func New(addresses []string, queue chan common.TraceItem) *Sender {
	s := &Sender{addrs: addresses, queue: queue}
	s.Go(s.send)
	return s
}
