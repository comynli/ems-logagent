package location

import (
	"code.google.com/p/leveldb-go/leveldb"
	"strconv"
	"sync"
)

type LocationServer struct {
	db    *leveldb.DB
	mutex *sync.RWMutex
}

func New(dirname string) (*LocationServer, error) {
	db, err := leveldb.Open(dirname, nil)
	if err != nil {
		return nil, err
	}
	ls := &LocationServer{db: db, mutex: &sync.RWMutex{}}
	return ls, nil
}

func (ls *LocationServer) Get(key string) ([]byte, error) {
	ls.mutex.RLock()
	defer ls.mutex.RUnlock()
	return ls.db.Get([]byte(key), nil)
}

func (ls *LocationServer) GetInt64(key string) (int64, error) {
	val, err := ls.Get(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(val), 10, 64)
}

func (ls *LocationServer) Set(key, val string) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	return ls.db.Set([]byte(key), []byte(val), nil)
}

func (ls *LocationServer) SetInt64(key string, val int64) error {
	v := strconv.AppendInt([]byte{}, val, 10)
	k := []byte(key)
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	return ls.db.Set(k, v, nil)
}
