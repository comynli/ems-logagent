package config

import (
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

type Conf struct {
	Path           []string       `path`
	EMSServer      []string       `ems_servers`
	Pattern        string         `pattern`
	Named          map[string]int `named`
	LocationDB     string         `location_db`
	MaxPendingSize int64          `max_pending_size`
}

func Load(cfg string) (Conf, error) {
	conf := Conf{}
	c, err := ioutil.ReadFile(cfg)
	if err != nil {
		return conf, err
	}
	err = yaml.Unmarshal(c, &conf)
	return conf, err
}
