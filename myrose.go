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

func New() (*Connection, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s?charset=%s", "test", "test", "tcp", "localhost", "3306", "20171118", "utf8")
	dbConnection, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	connection := new(Connection)
	connection.DB = dbConnection
	return connection, nil
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