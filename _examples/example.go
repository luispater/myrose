package main

import (
	"github.com/luispater/myrose"
	"fmt"
)

func main() {
	db, err := myrose.New()
	if err != nil {
		fmt.Println(err)
	}
	db.Table("first").Fields("i_hash AS nn", "i_hint as t").Order("i_hint", "DESC").Get()
	db.Table("first").Order("i_hash").Get()
}
