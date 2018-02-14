package myrose

import (
	"github.com/luispater/myrose/utils"
	"strings"
	"fmt"
	"errors"
	"database/sql"
)

type Table struct {
	connection      *Connection              // db connection
	schema          []map[string]interface{} // schema
	name            string                   // table name
	fieldList       []string                 // table filed list
	fields          []string                 // fields
	alias           []string                 // fields alias
	where           []interface{}            // where
	order           [][]string               // order
	limit           int                      // limit
	offset          int                      // offset
	join            []interface{}            // join
	distinct        bool                     // distinct
	group           []string                 // group
	having          []interface{}            // having
	data            interface{}              // data
	conditionValues map[string]interface{}   // query condition value
	errs            []error                  // errors
}

func (this *Table) Init(tableName string, connection *Connection) *Table {
	this.name = tableName
	this.connection = connection
	this.conditionValues = make(map[string]interface{})
	this.errs = make([]error, 0)

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

func (this *Table) addError(err string) {
	this.errs = append(this.errs, errors.New(err))
}

func (this *Table) Fields(fields ...string) *Table {
	for i := range fields {
		// TODO: 此处需要处理函数型字段名
		asIndex := strings.Index(strings.ToUpper(fields[i]), " AS ")
		var field, alias string
		if asIndex == -1 {
			field = fields[i]
			alias = fields[i]
		} else {
			field = fields[i][:asIndex]
			alias = fields[i][asIndex+4:]
		}
		if this.HasColumn(field) {
			this.fields = append(this.fields, fields[i])
			this.alias = append(this.alias, alias)
		} else {
			this.addError("Unknown `Fetch` column '" + field + "' in 'field list'")
		}
	}
	return this
}

func (this *Table) Group(group ...string) *Table {
	if this.group == nil {
		this.group = make([]string, 0)
	}
	for i := range group {
		if this.HasColumn(group[i]) {
			this.group = append(this.group, group[i])
		} else {
			this.addError("Unknown `Group By` column '" + group[i] + "' in 'field list'")
		}
	}
	return this
}

func (this *Table) Order(args ...interface{}) *Table {
	if this.order == nil {
		this.order = make([][]string, 0)
	}
	if len(args) > 2 {
		this.addError("`Order` method params error")
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
		this.addError("Unknown `Order By` column '" + columnName + "' in 'field list'")
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

func (this *Table) whereCommon(whereType string, args ...interface{}) *Table {
	if this.where == nil {
		this.where = make([]interface{}, 0)
	}
	argsLen := len(args)
	if argsLen < 2 {
		this.addError("Split column name in Where method")
	} else if (argsLen == 2) || (argsLen == 3) {
		// TODO: 此处需要处理函数型字段名
		if this.HasColumn(utils.ToStr(args[0])) {
			this.where = append(this.where, []interface{}{whereType, args})
		} else {
			this.addError("Unknown `Where` column '" + utils.ToStr(args[0]) + "' in 'field list'")
		}
	} else {
		this.addError("Too much `Where` conditions")
	}
	return this
}

func (this *Table) Where(args ...interface{}) *Table {
	return this.whereCommon("AND", args...)
}

func (this *Table) OrWhere(args ...interface{}) *Table {
	return this.whereCommon("OR", args...)
}

func (this *Table) joinCommon(joinType string, table *Table, thisTableColumn, joinTableColumn string) *Table {
	if this.join == nil {
		this.join = make([]interface{}, 0)
	}

	if !this.HasColumn(thisTableColumn) {
		this.addError("Unknown `Join` column '" + thisTableColumn + "' in table `" + this.name + "` 'field list'")
	} else if !table.HasColumn(joinTableColumn) {
		this.addError("Unknown `Join` column '" + joinTableColumn + "' in table `" + table.name + "` 'field list'")
	}

	joinDetail := make([]interface{}, 0)
	joinDetail = append(joinDetail, table)
	joinDetail = append(joinDetail, thisTableColumn)
	joinDetail = append(joinDetail, joinTableColumn)
	this.join = append(this.join, []interface{}{joinType, joinDetail})
	return this

}

func (this *Table) Join(table *Table, thisTableColumn, joinTableColumn string) *Table {
	return this.joinCommon("INNER", table, thisTableColumn, joinTableColumn)
}

func (this *Table) LeftJoin(table *Table, thisTableColumn, joinTableColumn string) *Table {
	return this.joinCommon("LEFT", table, thisTableColumn, joinTableColumn)
}

func (this *Table) RightJoin(table *Table, thisTableColumn, joinTableColumn string) *Table {
	return this.joinCommon("RIGHT", table, thisTableColumn, joinTableColumn)
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
		result = "(" + strSql + ")"
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

func (this *Table) parseWhere(tableName, strWhere string) string {
	var strCondition string
	for i := range this.where {
		arrayWhere := this.where[i].([]interface{})
		arrayCondition := arrayWhere[1].([]interface{})
		whereLen := len(arrayCondition)
		arrayWhereCondition := make([]string, 3)
		if whereLen == 2 { // columnName, value: `columnName`=1
			if (len(this.join) > 0) || (tableName != this.name) {
				arrayWhereCondition[0] = "`" + this.name + "`." + utils.ToStr(arrayCondition[0].(string))
			} else {
				arrayWhereCondition[0] = utils.ToStr(arrayCondition[0].(string))
			}

			arrayWhereCondition[1] = "="
			arrayWhereCondition[2] = this.getConditionName("WHERE", arrayWhereCondition[0], arrayCondition[1])
		} else if whereLen == 3 { // columnName, operation, value: `columnName`>=1
			operation := strings.ToUpper(utils.ToStr(arrayCondition[1]))
			if (len(this.join) > 0) || (tableName != this.name) {
				arrayWhereCondition[0] = "`" + this.name + "`." + utils.ToStr(arrayCondition[0].(string))
			} else {
				arrayWhereCondition[0] = utils.ToStr(arrayCondition[0].(string))
			}
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
		} else if whereLen == 4 { // table, columnName, operation, value: `table2`.`columnName`>=1

		}
		strCondition = utils.Implode(" ", arrayWhereCondition)

		if len(strWhere) > 0 {
			strWhere += " " + arrayWhere[0].(string) + " " + strCondition
		} else {
			strWhere += strCondition
		}
	}
	for i := range this.join {
		arrayJoin := this.join[i].([]interface{})
		joinDetail := arrayJoin[1].([]interface{})
		strWhere = joinDetail[0].(*Table).parseWhere(this.name, strWhere)
		for k, v := range joinDetail[0].(*Table).conditionValues {
			this.conditionValues[k] = v
		}
	}
	return strWhere
}

func (this *Table) parseJoin() string {
	strJoin := ""
	arrayJoins := make([]string, 0)
	for i := range this.join {
		arrayJoin := this.join[i].([]interface{})
		joinType := arrayJoin[0].(string)
		joinDetail := arrayJoin[1].([]interface{})
		strJoin := joinType + " JOIN `" + joinDetail[0].(*Table).name + "` ON `" + this.name + "`.`" + utils.ToStr(joinDetail[1]) + "`=`" + joinDetail[0].(*Table).name + "`.`" + utils.ToStr(joinDetail[2]) + "`"
		arrayJoins = append(arrayJoins, strJoin)
	}
	strJoin = utils.Implode(" ", arrayJoins)
	return strJoin
}

func (this *Table) parseGroup() string {
	strGroup := ""
	arrayGroup := make([]string, 0)
	for i := range this.group {
		if len(this.join) > 0 {
			arrayGroup = append(arrayGroup, "`"+this.name+"`.`"+this.group[i]+"`")
		} else {
			arrayGroup = append(arrayGroup, "`"+this.group[i]+"`")
		}
	}
	if len(arrayGroup) > 0 {
		strGroup = utils.Implode(", ", arrayGroup)
	}
	return strGroup
}

func (this *Table) buildQuery(queryType string) (string, map[string]interface{}) {
	strSql := ""
	if queryType == "SELECT" {
		strSql = "SELECT "
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
			strSql += utils.Implode(", ", arrayFields)
		} else {
			strSql += "`" + this.name + "`.*"
		}
		strSql += " FROM `" + this.name + "`"

		joinSql := this.parseJoin()
		if len(joinSql) > 0 {
			strSql += " " + joinSql
		}

		whereSql := this.parseWhere(this.name, "")
		if len(whereSql) > 0 {
			strSql += " WHERE " + whereSql
		}

		groupSql := this.parseGroup()
		if len(groupSql) > 0 {
			strSql += " GROUP BY " + groupSql
		}

		if len(this.order) > 0 {
			arrayOrders := make([]string, 0)
			for i := range this.order {
				orderSql := "`" + this.name + "`.`" + this.order[i][0] + "` " + this.order[i][1]
				arrayOrders = append(arrayOrders, orderSql)
			}
			strSql += " ORDER BY " + utils.Implode(", ", arrayOrders)
		}

		if this.limit > 0 {
			strSql += fmt.Sprintf(" LIMIT %d", this.limit)
		}
		if this.offset > 0 {
			strSql += fmt.Sprintf(" OFFSET %d", this.offset)
		}
	} else if queryType == "INSERT" {
		strSql = "INSERT INTO `" + this.name + "` SET "
		arrayInserts := make([]string, 0)
		for key, value := range this.data.(map[string]interface{}) {
			strInsert := "`" + key + "` = " + this.getConditionName("INSERT", key, value)
			arrayInserts = append(arrayInserts, strInsert)
		}
		strSql += utils.Implode(", ", arrayInserts)
	} else if queryType == "UPDATE" {
		strSql = "UPDATE `" + this.name + "` SET "

		arrayUpdates := make([]string, 0)
		for key, value := range this.data.(map[string]interface{}) {
			strUpdate := "`" + key + "` = " + this.getConditionName("INSERT", key, value)
			arrayUpdates = append(arrayUpdates, strUpdate)
		}
		strSql += utils.Implode(", ", arrayUpdates)

		whereSql := this.parseWhere(this.name, "")
		if len(whereSql) > 0 {
			strSql += " WHERE " + whereSql
		}
	} else if queryType == "DELETE" {
		strSql = "DELETE FROM `" + this.name + "`"
		whereSql := this.parseWhere(this.name, "")
		if len(whereSql) > 0 {
			strSql += " WHERE " + whereSql
		}
	}
	return strSql, this.conditionValues
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

func (this *Table) Execute(strSql string, mapArgv map[string]interface{}) (int64, error) {
	strSql, argv := utils.GetNamedSQL(strSql, mapArgv)
	fmt.Println(strSql, argv)
	var stmt *sql.Stmt
	var err error
	if this.connection.Transaction {
		stmt, err = this.connection.Tx.Prepare(strSql)
	} else {
		stmt, err = this.connection.DB.Prepare(strSql)
	}

	if err != nil {
		return 0, err
	}
	queryResult, errs := stmt.Exec(argv...)
	if errs != nil {
		stmt.Close()
		return 0, errs
	}
	var result int64
	switch strSql[:6] {
	case "INSERT":
		result, err = queryResult.LastInsertId()
	case "UPDATE":
		result, err = queryResult.RowsAffected()
	case "DELETE":
		result, err = queryResult.RowsAffected()
	}
	stmt.Close()
	return result, err
}

func (this *Table) Get() ([]map[string]interface{}, error) {
	strSql, argv := this.buildQuery("SELECT")
	return this.Query(strSql, argv)
}

func (this *Table) First() (map[string]interface{}, error) {
	limit := this.limit
	offset := this.offset
	defer func() { this.limit = limit; this.offset = offset }() //revert
	this.limit = 1
	this.offset = 0

	strSql, argv := this.buildQuery("SELECT")
	result, err := this.Query(strSql, argv)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0], nil
}

func (this *Table) Insert(data map[string]interface{}) (int64, error) {
	for key := range data {
		if !this.HasColumn(key) {
			return 0, errors.New("Unknown `Insert` column '" + key + "' in 'field list'")
		}
	}
	this.data = data
	strSql, argv := this.buildQuery("INSERT")
	return this.Execute(strSql, argv)
}

func (this *Table) Update(data map[string]interface{}) (int64, error) {
	for key := range data {
		if !this.HasColumn(key) {
			return 0, errors.New("Unknown `Update` column '" + key + "' in 'field list'")
		}
	}
	this.data = data
	strSql, argv := this.buildQuery("UPDATE")
	return this.Execute(strSql, argv)
}

func (this *Table) Delete() (int64, error) {
	strSql, argv := this.buildQuery("DELETE")
	return this.Execute(strSql, argv)
}
