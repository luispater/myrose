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
	a := db.Table("center").Where("ce_id", "IN", db.Table("customer").Fields("ce_id").Where("c_id", "IN", []int{54149471, 54204647}))
	b, err := a.Get()
	fmt.Println(b, err)

}
