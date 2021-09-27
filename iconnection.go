package dbpool

import "database/sql"

type IConnection interface {
	Query(query string, params ...interface{}) (rows *sql.Rows, err error)
	QueryRow(query string, params ...interface{}) (row *sql.Row)
	Exec(query string, params ...interface{}) (result sql.Result, err error)
	Ping() (err error)
	Close() (err error)
	GetType() (t DBType)
	close() (err error)
}
