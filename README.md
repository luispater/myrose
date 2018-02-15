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

```
更多的使用方法，请期待我的文档

### 授权协议

MIT

### 贡献及反馈

- [Issues](https://github.com/luispater/myrose/issues)
- [Pull requests](https://github.com/luispater/myrose/pulls)

### 更新日志

> 0.0.1

- 实现了查询器的基本功能，增删改查都有了
- 强制使用statement进行查询，提高了查询的安全性
- 强制对使用的数据表中的字段进行检查，对查询中不存在的字段，不允许提交查询
- 对Delete、Update等操作进行安全检查，当没有任何Where条件的情况下，不允许提交查询
- 可使用DeleteForce、UpdateForce强制提交没有任何Where条件的查询

### 开发计划

> 0.0.2

- 完成基础配置部分的代码
- 在README.md中添加使用代码
- Where及Having中允许使用函数
- 增加不允许使用原始查询（Query、Execute）开关
- UPDATE语句中增加类似a = a + 1的语法，原子操作，避免事务操作中的更新丢失（Lost Update）