# MyRose 数据库查询器构造器

### 这玩意是啥？

MyRose，是[Gorose](https://github.com/gohouse/gorose)的一个衍生品，但又与Gorose完全无关，只是借鉴了Gorose的设计思想（及小部分代码，在构件框架的时候使用了Gorose的方法名），并且将数据库的支持范围缩小到了MySQL及其衍生数据库。

查询器的设计思想仍然采用了 PHP 框架 Laravel 的设思想。

如果说GoRose是一个方便快捷易用的查询构造器（fizzday仁兄称之为ORM，我持保留意见），那MyRose是一款为了团队开发（特别是团队中有许多喜欢胡乱写SQL的成员的团队）量身定制的Golang的数据库查询构造器。

MyRose的一个特点就是约束，大量的字段检查，大量的条件约束，一不留神在提交SQL到MySQL之前，你就会不停的收到错误。

虽然我觉得是言过其实了，其实没这么吓人，开发这个构造器的目的是打算在团队开发中从 PHP 转换为 Golang，我也比较了许多同类的查询器、ORM，Gorose 是比较适合从 PHP 转到 Golang 的，但是在一个 DEMO 之后就发觉了不少问题，当然了，我也给 fizzday 仁兄提交了一些改进意见。

废话不多说，开始吧！

### 快速预览

```go
// SELECT * FROM `users` WHERE `id`=1 LIMIT 1
db.Table("users").Where("id",1).First()
// SELECT `id` AS uid, `name`, `age` FROM `users` WHERE `id`=1 ORDER BY `id` DESC, `name` ASC LIMIT 10
db.Table("users").Fields("id as uid", "name", "age").Where("id",1).Order("id", "desc").Order("name").Limit(10).Get()

// query string
db.Query("SELECT * FROM `user` LIMIT 10")
db.Execute("UPDATE `users` SET `name`='luispater' WHERE id=?", 1)
```

也许你会说这不就是Gorose的代码么？OK，再来点好玩的！
```go
//named parameter in the SQL statement 
db.Execute("UPDATE `users` SET `name`=:name WHERE `id`=:id", map[string]interface{}{"name":"luispater", "id":3})

//SELECT * FROM `table_1` WHERE `t1_id` IN (SELECT `t1_id` FROM `table_2` WHERE `t2_id` IN (1, 2))
db.Table("table_1").Where("t1_id", "IN", db.Table("table_2").Fields("t1_id").Where("t2_id", "IN", []int{1, 2}))
```

### 特性

- 使用查询构造器时进行强制字段约束
- 链式操作
- 连接池（这个不是我实现的……Golang自带了……）
- 标签参数（[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)，不乐意实现[点这里](https://github.com/go-sql-driver/mysql/issues/561)，我简单实现了一下）


### 依赖

- Golang 1.6+
- [Glide](https://glide.sh) (可选的，Golang的依赖管理工具)

### 安装

- Golang官方安装:  
```go
go get -u github.com/luispater/myrose
```
- 或者用 [Glide](https://glide.sh):  
```go
glide get github.com/luispater/myrose
```

### 简单的使用
```go
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
```
更多的使用方法，请期待我的文档

### 授权协议

MIT

### 贡献及反馈

- [Issues](https://github.com/luispater/myrose/issues)
- [Pull requests](https://github.com/luispater/myrose/pulls)

### 更新日志
> 1.0.2

- 为配合gopkg.in，将版本升级至1.0.2，代码无任何改编

> 0.0.2

- 增加不允许使用原始查询（Query、Execute）开关
- Where及Having中允许使用函数
- UPDATE语句中增加类似a = a + 1的语法，原子操作，避免事务操作中的更新丢失（Lost Update）

> 0.0.1

- 实现了查询器的基本功能，增删改查都有了
- 强制使用statement进行查询，提高了查询的安全性
- 强制对使用的数据表中的字段进行检查，对查询中不存在的字段，不允许提交查询
- 对Delete、Update等操作进行安全检查，当没有任何Where条件的情况下，不允许提交查询
- 可使用DeleteForce、UpdateForce强制提交没有任何Where条件的查询
- 完成基础配置部分的代码
- 在README.md中添加使用代码

### 开发计划

> 1.0.3

- 目前暂无开发计划