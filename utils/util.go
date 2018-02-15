package utils

import (
	"fmt"
	"strings"
	"sort"
	"crypto/md5"
	"io"
)

func InArray(needle interface{}, hystack interface{}) bool {
	switch key := needle.(type) {
	case string:
		for _, item := range hystack.([]string) {
			if key == item {
				return true
			}
		}
	case int:
		for _, item := range hystack.([]int) {
			if key == item {
				return true
			}
		}
	case int64:
		for _, item := range hystack.([]int64) {
			if key == item {
				return true
			}
		}
	default:
		return false
	}
	return false
}

func ToStr(obj interface{}) string {
	switch obj.(type) {
	case string:
		return fmt.Sprintf("%s", obj)
	case int:
		return fmt.Sprintf("%d", obj)
	case int64:
		return fmt.Sprintf("%d", obj)
	case float32:
		return fmt.Sprintf("%f", obj)
	case float64:
		return fmt.Sprintf("%f", obj)
	default:
		return ""
	}
}

func Implode(glue string, pieces []string) string {
	return strings.Join(pieces, glue)
}

func Md5(str string) string {
	h := md5.New()
	io.WriteString(h, str)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func GetNamedSQL(strSql string, argv map[string]interface{}) (string, []interface{}) {
	arrayIndexs := make([]int, 0)
	mapArgv := make(map[int]interface{})
	newArgv := make([]interface{}, 0)
	for key, value := range argv {
		intIndex := strings.Index(strSql, ":"+key)
		if intIndex!=-1 {
			arrayIndexs = append(arrayIndexs, intIndex)
			mapArgv[intIndex] = value
			strSql = strings.Replace(strSql, ":"+key, "?", 1)
		}
	}
	sort.Ints(arrayIndexs)
	for i := range arrayIndexs {
		newArgv = append(newArgv, mapArgv[arrayIndexs[i]])
	}
	return strSql, newArgv
}
