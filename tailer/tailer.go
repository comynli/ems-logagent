package tailer

import (
	"github.com/ActiveState/tail"
	"github.com/lixm/ems-logagent/location"
	tomb "gopkg.in/tomb.v2"
	"log"
	"os"
	"time"
)

type Tailer struct {
	tomb.Tomb
	t              *tail.Tail
	path           string
	queue          chan string
	file           *os.File
	location       *location.LocationServer
	maxPendingSize int64
}

func (t *Tailer) tail() error {

	for {
		select {
		case <-t.Dying():
			pos, err := t.t.Tell()
			if err != nil {
				t.location.SetInt64(t.path, pos)
			}
			return nil
		case <-time.After(time.Duration(3) * time.Second):
			t.checkPosition()
		case line := <-t.t.Lines:
			t.queue <- line.Text
		}
	}
}

func (t *Tailer) checkPosition() {
	pos, err := t.t.Tell()
	if err != nil {
		log.Printf("get current position of %s fail %s", t.path, err)
		return
	}
	t.location.SetInt64(t.path, pos)
	stat, err := t.file.Stat()
	if err != nil {
		log.Printf("get file size of %s fail %s", t.file.Name(), err)
		return
	}

	if stat.Name() != t.t.Filename {
		file, err := os.Open(t.path)
		if err != nil {
			return
		}
		t.file.Close()
		t.file = file
		return
	}

	size := stat.Size()
	if t.maxPendingSize > 0 && (size-pos) > t.maxPendingSize*1024*1024 {
		log.Println("%s overhead limit pending size, current pos is %d, size is %d\n", t.path, pos, size)
	}
}

func (t *Tailer) Stop() error {
	t.Kill(nil)
	return t.Wait()
}

func New(path string, queue chan string, loc *location.LocationServer, maxPendingSize int64) (*Tailer, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	offset, err := loc.GetInt64(path)
	if err != nil {
		stat, err := file.Stat()
		if err != nil {
			offset = 0
		} else {
			offset = stat.Size()
		}
	}

	tailer, err := tail.TailFile(path, tail.Config{
		Follow:   true,
		Location: &tail.SeekInfo{Offset: offset},
		ReOpen:   true,
		Poll:     false,
	})

	t := &Tailer{t: tailer, file: file, path: path, location: loc, queue: queue, maxPendingSize: maxPendingSize}
	t.Go(t.tail)
	return t, nil
}
