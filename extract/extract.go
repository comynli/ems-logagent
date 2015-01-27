package extract

import (
	"errors"
	"github.com/lixm/ems/common"
	tomb "gopkg.in/tomb.v2"
	"log"
	"regexp"
	"strconv"
)

type Extractor struct {
	re    *regexp.Regexp
	named map[string]int
	queue chan string
	out   chan common.TraceItem
	tomb.Tomb
}

func (e *Extractor) extract() error {
	for {
		select {
		case <-e.Dying():
			return nil
		case line := <-e.queue:
			ti, err := e.buildTraceItem(line)
			if err != nil {
				log.Printf("parse log %s fail %s", line, err)
				continue
			}
			e.out <- ti
		}
	}
}

func (e *Extractor) buildTraceItem(line string) (common.TraceItem, error) {
	ti := common.TraceItem{}
	metched := e.re.FindStringSubmatch(line)
	if len(metched) < len(e.named) {
		return ti, errors.New("not match")
	}
	ti.RequestId = metched[e.named["request_id"]]
	ti.Path = metched[e.named["path"]]
	status, err := strconv.ParseInt(metched[e.named["status"]], 10, 32)
	if err != nil {
		return ti, err
	}
	ti.Status = int(status)
	rt, err := strconv.ParseFloat(metched[e.named["rt"]], 64)
	if err != nil {
		return ti, err
	}
	ti.RT = int64(rt)
	return ti, nil
}

func (e *Extractor) Stop() error {
	e.Kill(nil)
	return e.Wait()
}

func New(pattern string, named map[string]int, in chan string, out chan common.TraceItem) *Extractor {
	re := regexp.MustCompile(pattern)
	e := &Extractor{re: re, named: named, queue: in, out: out}
	e.Go(e.extract)
	return e
}
