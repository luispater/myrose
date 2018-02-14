package myrose

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

type Connection struct {
	DB          *sql.DB
	Tx          *sql.Tx
	Transaction bool
	GlobalId    int64
}

var dbConnection *Connection

func newConnection() (*Connection, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s?charset=%s", "test", "test", "tcp", "localhost", "3306", "20171118", "utf8")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if dbConnection == nil {
		dbConnection = new(Connection)
	}
	dbConnection.DB = db
	dbConnection.DB.SetMaxIdleConns(10)
	dbConnection.DB.SetMaxOpenConns(50)
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
