package myrose

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	//"fmt"
)

type Connection struct {
	DB          *sql.DB
	Tx          *sql.Tx
	Transaction bool
	GlobalId    int64
}

var dbConnection *Connection

func newConnection(dsn string) (*Connection, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if dbConnection == nil {
		dbConnection = new(Connection)
	}
	dbConnection.DB = db
	return dbConnection, nil
}

//dsn format: root:@tcp(localhost:3306)/test?charset=utf8
func New(dsn string) (*Connection, error) {
	if dbConnection != nil {
		err := dbConnection.DB.Ping()
		if err == nil {
			return dbConnection, nil
		} else {
			return newConnection(dsn)
		}
	} else {
		return newConnection(dsn)
	}
}

func NewData() map[string]interface{} {
	return make(map[string]interface{})
}

func (this *Connection) SetMaxIdleConns(n int) {
	this.DB.SetMaxIdleConns(10)
}

func (this *Connection) SetMaxOpenConns(n int) {
	this.DB.SetMaxOpenConns(50)
}

func (this *Connection) Close() error {
	return this.DB.Close()
}

func (this *Connection) Ping() error {
	return this.DB.Ping()
}

func (this *Connection) Begin() error {
	tx, err := this.DB.Begin()
	if err == nil {
		this.Tx = tx
		this.Transaction = true
	}
	return err
}

func (this *Connection) Commit() error {
	err := this.Tx.Commit()
	if err == nil {
		this.Transaction = false
	}
	return err
}

func (this *Connection) Rollback() error {
	err := this.Tx.Rollback()
	if err == nil {
		this.Transaction = false
	}
	return err
}

func (this *Connection) Table(name string) *Table {
	return new(Table).Init(name, this)
}
