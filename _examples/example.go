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
	//a := db.Table("center").Where("ce_id", "IN", db.Table("customer").Fields("ce_id").Where("c_id", "IN", []int{54149471, 54204647}))
	//b, err := a.Get()
	//fmt.Println(b, err)
	tableCenter := db.Table("center")
	tableCenter.Where("ce_id", ">", 0)
	a := db.Table("customer").RightJoin(tableCenter, "ce_id", "c_id").Where("ce_id", 1)
	a.Group("ce_id", "c_id")
	a.Order().
	a.Limit(10)
	a.Offset(10)
	_, err = a.First()
	fmt.Println(err)
	_, err = a.Get()
	fmt.Println(err)

}
