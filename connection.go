package dbpool

import (
	"database/sql"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type connection struct {
	recycleChannel chan IConnection
	conn           *sql.DB
	lock           sync.Mutex
	dbType         DBType
}

func newConnection(recycle chan IConnection, config DBConfig) (conn IConnection, err error) {

	var (
		db *sql.DB
	)

	if db, err = sql.Open(string(config.ConnType), config.GetConnectionQuery()); err != nil {
		return
	}

	conn = &connection{
		recycleChannel: recycle,
		conn:           db,
		dbType:         config.ConnType,
	}

	return
}

func (thisObj *connection) Query(query string, params ...interface{}) (rows *sql.Rows, err error) {
	rows, err = thisObj.conn.Query(query, params...)
	return
}

func (thisObj *connection) QueryRow(query string, params ...interface{}) (row *sql.Row) {
	row = thisObj.conn.QueryRow(query, params...)
	return
}

func (thisObj *connection) Exec(query string, params ...interface{}) (result sql.Result, err error) {
	result, err = thisObj.conn.Exec(query, params...)
	return
}

func (thisObj *connection) Ping() (err error) {
	return thisObj.conn.Ping()
}

func (thisObj *connection) Close() (err error) {
	thisObj.recycleChannel <- thisObj
	return
}

func (thisObj *connection) GetType() (t DBType) {
	t = thisObj.dbType
	return
}

// real *sql.DB close
func (thisObj *connection) close() (err error) {
	thisObj.lock.Lock()
	defer thisObj.lock.Unlock()
	err = thisObj.conn.Close()
	return
}
