package myrose

import (
	"github.com/luispater/myrose/utils"
	"strings"
	"fmt"
)

type Table struct {
	connection *Connection              //db connection
	schema     []map[string]interface{} //schema
	name       string                   // table name
	fieldList  []string                 //table filed list
	fields     []string                 // fields
	where      []interface{}            // where
	order      [][]string               // order
	limit      int                      // limit
	offset     int                      // offset
	join       []map[*Table]interface{} // join
	distinct   bool                     // distinct
	group      []string                 // group
	having     [][]interface{}          // having
	data       interface{}              // data
}

func (this *Table) Init(tableName string, connection *Connection) *Table {
	this.name = tableName
	this.connection = connection

	rows, err := connection.DB.Query("DESC `" + tableName + "`")
	if err != nil {
		this.schema = nil
	}
	columns, err := rows.Columns()
	if err != nil {
		this.schema = nil
	}
	columnsNum := len(columns)

	values := make([]interface{}, columnsNum)
	scanDests := make([]interface{}, columnsNum)

	schema := make([]map[string]interface{}, 0)
	for rows.Next() {
		for i := 0; i < columnsNum; i++ {
			scanDests[i] = &values[i]
		}
		rows.Scan(scanDests...)
		row := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			if data, ok := val.([]byte); ok {
				v = string(data)
			} else {
				v = val
			}
			row[col] = v
		}
		schema = append(schema, row)
	}
	this.schema = schema
	fileds := make([]string, 0)
	for i := range schema {
		fileds = append(fileds, schema[i]["Field"].(string))
	}
	this.fieldList = fileds
	return this
}

func (this *Table) SetConnection(connection *Connection) {
	this.connection = connection
}

func (this *Table) HasColumn(column string) bool {
	return utils.InArray(column, this.fieldList)
}

func (this *Table) Fields(fields ...string) *Table {
	for i := range fields {
		asIndex := strings.Index(strings.ToUpper(fields[i]), " AS ")
		var field string
		if asIndex == -1 {
			field = fields[i]
		} else {
			field = fields[i][:asIndex]
		}
		if this.HasColumn(field) {
			this.fields = append(this.fields, fields[i])
		} else {
			panic("Unknown `Fetch` column '" + field + "' in 'field list'")
		}
	}
	return this
}

func (this *Table) Data(data interface{}) *Table {
	return this
}

func (this *Table) Group(group []string) *Table {
	if this.group == nil {
		this.group = make([]string, 0)
	}
	for i := range group {
		if this.HasColumn(group[i]) {
			this.group = append(this.group, group[i])
		} else {
			panic("Unknown `Group By` column '" + group[i] + "' in 'field list'")
		}
	}
	return this
}

func (this *Table) Order(args ...interface{}) *Table {
	if this.order == nil {
		this.order = make([][]string, 0)
	}
	if len(args) > 2 {
		panic("`Order` method params error.")
	}
	var columnName, sequence string
	if len(args) == 1 {
		columnName = utils.ToStr(args[0])
		sequence = "ASC"
	} else {
		columnName = utils.ToStr(args[0])
		sequence = strings.ToUpper(utils.ToStr(args[1]))
	}
	if this.HasColumn(columnName) {
		sequence = strings.ToUpper(sequence)
		if utils.InArray(sequence, []string{"ASC", "DESC"}) {
			this.order = append(this.order, []string{columnName, sequence})
		} else {
			this.order = append(this.order, []string{columnName, "ASC"})
		}
	} else {
		panic("Unknown `Order By` column '" + columnName + "' in 'field list'")
	}

	return this
}

func (this *Table) Limit(limit int) *Table {
	this.limit = limit
	return this
}

func (this *Table) Offset(offset int) *Table {
	this.offset = offset
	return this
}

func (this *Table) Where(args ...interface{}) *Table {
	if this.where == nil {
		this.where = make([]interface{}, 0)
	}
	argsLen := len(args)
	if argsLen < 2 {
		panic("Split column name in Where method")
	} else if argsLen == 2 {
		if this.HasColumn(utils.ToStr(args[0])) {
			this.where = append(this.where, []interface{}{"and", args})
		} else {
			panic("Unknown `Where` column '" + utils.ToStr(args[0]) + "' in 'field list'")
		}
	}
	return this
}

func (this *Table) OrWhere(args ...interface{}) *Table {
	if this.where == nil {
		this.where = make([]interface{}, 0)
	}
	argsLen := len(args)
	if argsLen < 2 {
		panic("Split column name in OrWhere method")
	} else if argsLen == 2 {
		if utils.InArray(utils.ToStr(args[0]), this.fieldList) {
			this.where = append(this.where, []interface{}{"and", args})
		} else {
			panic("Unknown `Where` column '" + utils.ToStr(args[0]) + "' in 'field list'")
		}
	}
	return this
}

func (this *Table) Join(table *Table, on ...interface{}) *Table {
	if this.join == nil {
		this.join = make([]map[*Table]interface{}, 0)
	}
	return this
}

func (this *Table) LeftJoin(table *Table, on ...interface{}) *Table {
	if this.join == nil {
		this.join = make([]map[*Table]interface{}, 0)
	}
	return this
}

func (this *Table) RightJoin(table *Table, on ...interface{}) *Table {
	if this.join == nil {
		this.join = make([]map[*Table]interface{}, 0)
	}
	return this
}

func (this *Table) Get() ([]map[string]interface{}, error) {
	fmt.Println(this.buildQuery("SELECT"))
	return nil, nil
}

func (this *Table) parseWhere() string {
	return ""
}

func (this *Table) buildQuery(queryType string) string {
	sql := ""
	if queryType == "SELECT" {
		sql = "SELECT "
	}
	if len(this.fields) > 0 {
		arrayFields := make([]string, 0)
		for i := range this.fields {
			asIndex := strings.Index(strings.ToUpper(this.fields[i]), " AS ")
			var field string
			if asIndex == -1 {
				field = this.fields[i]
				fieldSql := "`" + this.name + "`.`" + field + "`"
				arrayFields = append(arrayFields, fieldSql)
			} else {
				field = this.fields[i][:asIndex]
				fieldSql := "`" + this.name + "`.`" + field + "` AS " + this.fields[i][asIndex+4:]
				arrayFields = append(arrayFields, fieldSql)
			}
		}
		sql += utils.Implode(", ", arrayFields)
	} else {
		sql += "`" + this.name + "`.*"
	}
	sql += " FROM `" + this.name + "`"

	whereSql := this.parseWhere()
	if len(whereSql)>0 {
		sql += " WHERE " + whereSql
	}

	if len(this.order) > 0 {
		arrayOrders := make([]string, 0)
		for i := range this.order {
			orderSql := "`" + this.name + "`.`" + this.order[i][0] + "` " + this.order[i][1]
			arrayOrders = append(arrayOrders, orderSql)
		}
		sql += " ORDER BY " + utils.Implode(", ", arrayOrders)
	}
	return sql
}
