package main

import (
	"github.com/luispater/myrose"
	"github.com/luispater/myrose/utils"
	"fmt"
)

func main() {
	// connect
	dsn := "test:test@unix(/tmp/mysql.sock)/myrose?charset=utf8"
	db, err := myrose.New(dsn)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// insert
	insert := myrose.NewData()
	insert["name"] = "luis"
	insert["password"] = utils.Md5("luis")
	insert["status"] = 1
	intInsertId, err := db.Table("users").Insert(insert)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Insert Id: %d\n", intInsertId)
	}

	// first row query
	res, err := db.Table("users").First()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)

	// update
	update := myrose.NewData()
	update["password"] = utils.Md5("luis1")
	update["status"] = 0
	intAffectedNum, err := db.Table("users").Where("name", "=", "luis").Update(update)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Update Affected Number: %d\n", intAffectedNum)
	}

	// all row query
	rows, err := db.Table("users").Where("status", "=", 0).Get()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(rows)

	// add test data
	for i:=0; i<1000; i++ {
		insert := myrose.NewData()
		insert["name"] = fmt.Sprintf("luis_%d", i)
		insert["password"] = utils.Md5("luis")
		insert["status"] = 1
		intInsertId, err := db.Table("users").Insert(insert)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("Insert Id: %d\n", intInsertId)
			insert := myrose.NewData()
			insert["user_sex"] = "female"
			insert["user_id"] = intInsertId
			_, err := db.Table("users_info").Insert(insert)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	db.Begin()
	// all row query
	intAffectedNum, err = db.Table("users").Where("id", ">=", 500).Delete()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Delete Affected Number: %d\n", intAffectedNum)
	}
	db.Rollback()

	db.Begin()
	// all row query
	intAffectedNum, err = db.Table("users").Where("id", ">=", 500).Limit(50).Delete()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Delete Affected Number: %d\n", intAffectedNum)
	}
	db.Rollback()

	// left join
	row, err := db.Table("users").LeftJoin(db.Table("users_info"), "id", "user_id").Where("id", "=", 101).First()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(row)

	// native mysql function
	rows, err = db.Table("users").Fields("count(*) as count", "sum(id) as sum").Group("password").Having("sum", ">", 1).Get()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(rows)
}
