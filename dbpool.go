package dbpool

import (
	"fmt"
	"sync"
)

var (
	pool     map[string]*connections = make(map[string]*connections)
	poolLock sync.RWMutex
)

func New(pName string, config DBConfig) (err error) {
	if pName == "" {
		err = fmt.Errorf("parameter pName is empty , it should be pool name")
		return
	}

	if err = config.Check(); err != nil {
		return
	}

	if Exist(pName) {
		err = fmt.Errorf("pool %s had already exist", pName)
		return
	}

	poolLock.Lock()
	defer poolLock.Unlock()
	pool[pName], err = newConnections(config)
	return
}

func Get(pName string) (db *connection, err error) {

	if pName == "" {
		err = fmt.Errorf("parameter pName is empty , it should be pool name")
		return
	}

	if !Exist(pName) {
		err = fmt.Errorf("pool %s is not exist", pName)
		return
	}

	poolLock.Lock()
	defer poolLock.Unlock()
	db, err = pool[pName].getConnection()
	return
}

func Remove(pName string) (err error) {
	if pName == "" {
		err = fmt.Errorf("parameter pName is empty , it should be pool name")
		return
	}

	if !Exist(pName) {
		err = fmt.Errorf("pool %s is not exist", pName)
		return
	}

	poolLock.Lock()
	defer poolLock.Unlock()

	if err = pool[pName].close(); err != nil {
		return
	}

	delete(pool, pName)

	return
}

func Exist(pName string) (isExist bool) {
	if pName == "" {
		return
	}
	poolLock.RLock()
	defer poolLock.RUnlock()
	_, isExist = pool[pName]
	return
}
