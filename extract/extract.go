package extract

import (
	"errors"
	"github.com/lixm/ems/common"
	tomb "gopkg.in/tomb.v2"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	timeFormat = "[2/Jan/2006:15:04:05 -0700]"
)

type Extractor struct {
	named map[string]int
	queue chan string
	out   chan common.LogItem
	tomb.Tomb
}

func (e *Extractor) extract() error {
	for {
		select {
		case <-e.Dying():
			return nil
		case line := <-e.queue:
			ti, err := e.buildLogItem(line)
			if err != nil {
				log.Printf("parse log %s fail %s", line, err)
				continue
			}
			e.out <- ti
		}
	}
}

func (e *Extractor) buildLogItem(line string) (common.LogItem, error) {
	li := common.LogItem{}
	raw := strings.Split(line, "\t")
	if len(raw) > e.named["request_id"] {
		li.RequestId = raw[e.named["request_id"]]
	} else {
		return li, errors.New("no request_id")
	}

	if len(raw) > e.named["status"] {
		tmp := strings.Split(raw[e.named["status"]], " ")
		status, err := strconv.ParseInt(tmp[0], 10, 32)
		if err != nil {
			return li, err
		}
		li.Status = int(status)
	} else {
		return li, errors.New("no status")
	}

	if len(raw) > e.named["rt"] {
		rt, err := strconv.ParseFloat(raw[e.named["rt"]], 64)
		if err != nil {
			return li, err
		}
		li.RT = int64(rt)
	} else {
		return li, errors.New("no rt")
	}
	if len(raw) > e.named["timestamp"] {
		t, err := time.Parse(timeFormat, raw[e.named["timestamp"]])
		if err != nil {
			return li, err
		}
		li.TimeStamp = t
	} else {
		return li, errors.New("no timestamp")
	}

	if len(raw) > e.named["host"] {
		li.Host = raw[e.named["host"]]
	} else {
		return li, errors.New("no host")
	}

	if len(raw) > e.named["path"] {
		request := raw[e.named["path"]]
		request = request[1 : len(request)-1]
		r := strings.Split(request, " ")
		if r[0] != "CONNECT" {
			query := strings.Split(r[1], "?")
			li.Path = query[0]
		} else {
			return li, errors.New("CONNECT request")
		}

	} else {
		return li, errors.New("no request_id")
	}

	return li, nil
}

func (e *Extractor) Stop() error {
	e.Kill(nil)
	return e.Wait()
}

func New(named map[string]int, in chan string, out chan common.LogItem) *Extractor {
	e := &Extractor{named: named, queue: in, out: out}
	e.Go(e.extract)
	return e
}
