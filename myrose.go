package myrose

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

type Connection struct {
	DB *sql.DB
	Tx *sql.Tx
	Transaction bool
	GlobalId int64
}

var dbConnection *Connection

func newConnection() (*Connection, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s?charset=%s", "test", "test", "tcp", "localhost", "3306", "20171118", "utf8")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if dbConnection==nil {
		dbConnection = new(Connection)
	}
	dbConnection.DB = db
	return dbConnection, nil
}

func New() (*Connection, error) {
	if dbConnection != nil {
		err := dbConnection.DB.Ping()
		if err == nil {
			return dbConnection, nil
		} else {
			return newConnection()
		}
	} else {
		return newConnection()
	}
}

func (this *Connection) Close() error {
	return this.DB.Close()
}

func (this *Connection) Ping() error {
	return this.DB.Ping()
}

func (this *Connection) Begin() {
	this.Tx, _ = this.DB.Begin()
	this.Transaction = true
}

func (this *Connection) Commit() {
	this.Tx.Commit()
	this.Transaction = false
}

func (this *Connection) Rollback() {
	this.Tx.Rollback()
	this.Transaction = false
}

func (this *Connection) Table(name string) *Table {
	return new(Table).Init(name, this)
}