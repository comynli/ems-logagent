package main

import (
	"github.com/lixm/ems-logagent/config"
	"github.com/lixm/ems-logagent/extract"
	"github.com/lixm/ems-logagent/location"
	"github.com/lixm/ems-logagent/send"
	"github.com/lixm/ems-logagent/tailer"
	"github.com/lixm/ems/common"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var conf config.Conf

func init() {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalln(err)
	}
	conf, err = config.Load((filepath.Join(dir, "ems.yml")))
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	ls, err := location.New(conf.LocationDB)
	if err != nil {
		log.Fatalln(err)
	}
	originQueue := make(chan string)
	traceItemQueue := make(chan common.TraceItem)
	sender := send.New(conf.EMSServer, traceItemQueue)
	extractor := extract.New(conf.Pattern, conf.Named, originQueue, traceItemQueue)
	tailers := []*tailer.Tailer{}
	for _, path := range conf.Path {
		t, err := tailer.New(path, originQueue, ls, conf.MaxPendingSize)
		if err != nil {
			log.Fatalln(err)
		}
		tailers = append(tailers, t)
	}

	sig := <-c
	log.Printf("%s received, exiting", sig.String())
	for _, t := range tailers {
		t.Stop()
	}
	extractor.Stop()
	sender.Stop()
}
