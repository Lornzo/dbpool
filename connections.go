package dbpool

import (
	"fmt"
	"sync"
)

type connections struct {
	config DBConfig

	stack []IConnection

	lock    sync.RWMutex
	recycle chan IConnection

	recycleWait     int
	recycleWaitLock sync.RWMutex
	waitRecycle     chan IConnection

	createdLock sync.RWMutex
	createdConn int
}

func newConnections(dbConfig DBConfig) (conns *connections, err error) {
	conns = &connections{
		config:      dbConfig,
		recycle:     make(chan IConnection),
		waitRecycle: make(chan IConnection),
	}
	go conns.recycleHandler()
	return
}

func (thisObj *connections) getConnection() (conn IConnection, err error) {

	if conn, err = thisObj.getFromStack(); err == nil {
		return
	}

	if conn, err = thisObj.openOne(); err == nil {
		return
	}

	if conn, err = thisObj.waitOne(); err == nil {
		return
	}

	conn, err = thisObj.getConnection()

	return

}

func (thisObj *connections) waitOne() (conn IConnection, err error) {

	thisObj.recycleWaitLock.Lock()
	thisObj.recycleWait++
	thisObj.recycleWaitLock.Unlock()

	conn = <-thisObj.waitRecycle

	thisObj.recycleWaitLock.Lock()
	thisObj.recycleWait--
	thisObj.recycleWaitLock.Unlock()

	if err = conn.Ping(); err != nil {
		thisObj.closeOne(conn)
	}

	return
}

func (thisObj *connections) openOne() (conn IConnection, err error) {
	thisObj.createdLock.Lock()
	defer thisObj.createdLock.Unlock()

	if thisObj.createdConn >= int(thisObj.config.ConnLimit) {
		err = fmt.Errorf("connection create limit")
		return
	}

	if conn, err = newConnection(thisObj.recycle, thisObj.config); err == nil {
		thisObj.createdConn++
	}

	return
}

func (thisObj *connections) recycleHandler() {
	var (
		waitNum int
	)

	for {
		conn := <-thisObj.recycle
		thisObj.recycleWaitLock.RLock()
		waitNum = thisObj.recycleWait
		thisObj.recycleWaitLock.RUnlock()
		if waitNum > 0 {
			thisObj.waitRecycle <- conn
		} else {
			thisObj.addToStack(conn)
		}

	}
}

func (thisObj *connections) addToStack(conn IConnection) {
	thisObj.lock.Lock()
	defer thisObj.lock.Unlock()
	thisObj.stack = append(thisObj.stack, conn)
}

func (thisObj *connections) getFromStack() (conn IConnection, err error) {

	thisObj.lock.Lock()

	var (
		connNum int = len(thisObj.stack)
	)

	if connNum > 0 {
		conn = thisObj.stack[0]
		if connNum > 1 {
			thisObj.stack = thisObj.stack[1:]
		} else {
			thisObj.stack = make([]IConnection, 0)
		}
	} else {
		err = fmt.Errorf("stack is empty")
	}

	thisObj.lock.Unlock()

	// if stack is empty , then return
	if err != nil {
		return
	}

	if err = conn.Ping(); err != nil {
		thisObj.closeOne(conn)
		conn, err = thisObj.getFromStack()
	}

	return

}

func (thisObj *connections) closeOne(conn IConnection) (err error) {
	thisObj.createdLock.Lock()
	defer thisObj.createdLock.Unlock()
	thisObj.createdConn--
	conn.close()
	return
}

func (thisObj *connections) close() (err error) {
	thisObj.lock.Lock()
	defer thisObj.lock.Unlock()
	for _, conn := range thisObj.stack {
		err = conn.close()
	}
	return
}
