package myrose

import (
	"github.com/luispater/myrose/utils"
	"strings"
	"fmt"
)

type Table struct {
	connection      *Connection              // db connection
	schema          []map[string]interface{} // schema
	name            string                   // table name
	fieldList       []string                 // table filed list
	fields          []string                 // fields
	where           []interface{}            // where
	order           [][]string               // order
	limit           int                      // limit
	offset          int                      // offset
	join            []map[*Table]interface{} // join
	distinct        bool                     // distinct
	group           []string                 // group
	having          [][]interface{}          // having
	data            interface{}              // data
	conditionValues map[string]interface{}   // query condition value
}

func (this *Table) Init(tableName string, connection *Connection) *Table {
	this.name = tableName
	this.connection = connection
	this.conditionValues = make(map[string]interface{})

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
	rows.Close()
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
	} else if (argsLen == 2) || (argsLen == 3) {
		if this.HasColumn(utils.ToStr(args[0])) {
			this.where = append(this.where, []interface{}{"AND", args})
		} else {
			panic("Unknown `Where` column '" + utils.ToStr(args[0]) + "' in 'field list'")
		}
	} else {
		panic("Too much `Where` conditions")
	}
	return this
}

func (this *Table) OrWhere(args ...interface{}) *Table {
	if this.where == nil {
		this.where = make([]interface{}, 0)
	}
	argsLen := len(args)
	if argsLen < 2 {
		panic("Split column name in Where method")
	} else if (argsLen == 2) || (argsLen == 3) {
		if this.HasColumn(utils.ToStr(args[0])) {
			this.where = append(this.where, []interface{}{"OR", args})
		} else {
			panic("Unknown `Where` column '" + utils.ToStr(args[0]) + "' in 'field list'")
		}
	} else {
		panic("Too much `Where` conditions")
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
	strSql, argv := this.buildQuery("SELECT")
	return this.Query(strSql, argv)
}

func (this *Table) Query(strSql string, mapArgv map[string]interface{}) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0)
	strSql, argv := utils.GetNamedSQL(strSql, mapArgv)
	fmt.Println(strSql, argv)
	stmt, err := this.connection.DB.Prepare(strSql)
	if err != nil {
		return results, err
	}

	rows, err := stmt.Query(argv ...)
	if err != nil {
		return results, err
	}
	stmt.Close()

	columns, err := rows.Columns()
	if err != nil {
		return results, err
	}
	count := len(columns)
	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)

	for rows.Next() {
		for i := 0; i < count; i++ {
			scanArgs[i] = &values[i]
		}
		rows.Scan(scanArgs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			if b, ok := val.([]byte); ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		results = append(results, entry)
	}
	rows.Close()
	return results, nil
}

func (this *Table) getConditionName(prefix, columnName string, value interface{}) string {
	this.connection.GlobalId++
	strConditionName := fmt.Sprintf("%s_%d_%s", prefix, this.connection.GlobalId, columnName)
	this.conditionValues[strConditionName] = value
	return ":" + strConditionName
}

func (this *Table) parseInCondition(fieldName string, argv interface{}) string {
	result := ""
	switch argv.(type) {
	case *Table:
		strSql, mapArgv := argv.(*Table).buildQuery("SELECT")
		for k, v := range mapArgv {
			this.conditionValues[k] = v
		}
		result =  "(" + strSql + ")"
	case []int64:
		arrayConditionNames := make([]string, 0)
		for i := range argv.([]int64) {
			arrayConditionNames = append(arrayConditionNames, this.getConditionName("WHERE", fieldName, argv.([]int64)[i]))
		}
		result = "(" + utils.Implode(", ", arrayConditionNames) + ")"
	case []int:
		arrayConditionNames := make([]string, 0)
		for i := range argv.([]int) {
			arrayConditionNames = append(arrayConditionNames, this.getConditionName("WHERE", fieldName, argv.([]int)[i]))
		}
		result = "(" + utils.Implode(", ", arrayConditionNames) + ")"
	case []string:
		arrayConditionNames := make([]string, 0)
		for i := range argv.([]string) {
			arrayConditionNames = append(arrayConditionNames, this.getConditionName("WHERE", fieldName, argv.([]string)[i]))
		}
		result = "(" + utils.Implode(", ", arrayConditionNames) + ")"
	case []float64:
		arrayConditionNames := make([]string, 0)
		for i := range argv.([]float64) {
			arrayConditionNames = append(arrayConditionNames, this.getConditionName("WHERE", fieldName, argv.([]float64)[i]))
		}
		result = "(" + utils.Implode(", ", arrayConditionNames) + ")"
	case []float32:
		arrayConditionNames := make([]string, 0)
		for i := range argv.([]float32) {
			arrayConditionNames = append(arrayConditionNames, this.getConditionName("WHERE", fieldName, argv.([]float32)[i]))
		}
		result = "(" + utils.Implode(", ", arrayConditionNames) + ")"
	case []interface{}:
		arrayConditionNames := make([]string, 0)
		for i := range argv.([]interface{}) {
			arrayConditionNames = append(arrayConditionNames, this.getConditionName("WHERE", fieldName, argv.([]interface{})[i]))
		}
		result = "(" + utils.Implode(", ", arrayConditionNames) + ")"
	}
	return result
}

func (this *Table) parseWhere() string {
	strWhere := ""
	var strCondition string
	for i := range this.where {
		arrayWhere := this.where[i].([]interface{})
		arrayCondition := arrayWhere[1].([]interface{})
		whereLen := len(arrayCondition)
		arrayWhereCondition := make([]string, 3)
		if whereLen == 2 {
			arrayWhereCondition[0] = utils.ToStr(arrayCondition[0].(string))
			arrayWhereCondition[1] = "="
			arrayWhereCondition[2] = this.getConditionName("WHERE", arrayWhereCondition[0], arrayCondition[1])
		} else if whereLen == 3 {
			operation := strings.ToUpper(utils.ToStr(arrayCondition[1]))
			arrayWhereCondition[0] = utils.ToStr(arrayCondition[0].(string))
			arrayWhereCondition[1] = operation

			switch operation {
			case "LIKE":
				arrayWhereCondition[2] = this.getConditionName("WHERE", arrayWhereCondition[0], arrayCondition[2])
			case "NOT LIKE":
				arrayWhereCondition[2] = this.getConditionName("WHERE", arrayWhereCondition[0], arrayCondition[2])
			case "IN":
				arrayWhereCondition[2] = this.parseInCondition(arrayWhereCondition[0], arrayCondition[2])
			case "NOT IN":
				arrayWhereCondition[2] = this.parseInCondition(arrayWhereCondition[0], arrayCondition[2])
			case "IS":
				arrayWhereCondition[2] = "NULL"
			case "IS NOT":
				arrayWhereCondition[2] = "NULL"
			case "BETWEEN":
			case "NOT BETWEEN":
			default:
				arrayWhereCondition[2] = this.getConditionName("WHERE", arrayWhereCondition[0], arrayCondition[2])
			}
		}
		strCondition = utils.Implode(" ", arrayWhereCondition)

		if i > 0 {
			strWhere += " " + arrayWhere[0].(string) + " " + strCondition
		} else {
			strWhere += strCondition
		}
	}
	return strWhere
}

func (this *Table) buildQuery(queryType string) (string, map[string]interface{}) {
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
	if len(whereSql) > 0 {
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

	return sql, this.conditionValues
}
