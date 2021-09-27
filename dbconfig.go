package dbpool

import "fmt"

type DBConfig struct {
	ConnName    string `json:"ConnName"`
	ConnLimit   int64  `json:"ConnLimit"`
	ConnType    DBType `json:"ConnType"`
	ConnHost    string `json:"ConnHost"`
	DBPrefix    string `json:"DBPrefix"`
	DBName      string `json:"DBName"`
	DBUser      string `json:"DBUser"`
	DBPass      string `json:"DBPass"`
	TablePrefix string `json:"TablePrefix"`
	Charset     string `json:"Charset"`
}

// @return {string} NamePrefix + Name
func (thisObj *DBConfig) GetDBName() (dbName string) {
	dbName = thisObj.DBPrefix + thisObj.DBName
	return
}

func (thisObj *DBConfig) Check() (err error) {

	if thisObj.ConnLimit <= 0 {
		thisObj.ConnLimit = 100
	}

	switch thisObj.ConnType {
	case DB_TYPE_MYSQL:
		err = thisObj.checkMySQL()
	default:
		err = fmt.Errorf("db type %s is not support yet or empty", thisObj.ConnType)
	}
	return
}

func (thisObj *DBConfig) GetConnectionQuery() (query string) {
	switch thisObj.ConnType {
	case DB_TYPE_MYSQL:
		query = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s", thisObj.DBUser, thisObj.DBPass, thisObj.ConnHost, thisObj.GetDBName(), thisObj.Charset)
	}
	return
}

func (thisObj *DBConfig) checkMySQL() (err error) {
	if thisObj.Charset == "" {
		thisObj.Charset = "utf8"
	}
	return
}
