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

//noinspection GoUnusedExportedFunction
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
		if intIndex != -1 {
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

//noinspection GoUnusedExportedFunction
func IsInt(obj interface{}) bool {
	return DataType(obj)=="int"
}

//noinspection GoUnusedExportedFunction
func IsInt64(obj interface{}) bool {
	return DataType(obj)=="int64"
}

//noinspection GoUnusedExportedFunction
func IsString(obj interface{}) bool {
	return DataType(obj)=="string"
}

//noinspection GoUnusedExportedFunction
func IsFloat(obj interface{}) bool {
	return DataType(obj)=="float32"
}

//noinspection GoUnusedExportedFunction
func IsFloat64(obj interface{}) bool {
	return DataType(obj)=="float64"
}

func DataType(obj interface{}) string {
	switch obj.(type) {
	case string:
		return "string"
	case int:
		return "int"
	case int64:
		return "int64"
	case float32:
		return "float32"
	case float64:
		return "float64"

	case []string:
		return "[]string"
	case []int:
		return "[]int"
	case []int64:
		return "[]int64"
	case []float32:
		return "[]float32"
	case []float64:
		return "[]float64"

	case map[string]string:
		return "map[string]string"
	case map[string]int:
		return "map[string]int"
	case map[string]int64:
		return "map[string]int64"
	case map[string]float32:
		return "map[string]float32"
	case map[string]float64:
		return "map[string]float64"

	case map[int]string:
		return "map[int]string"
	case map[int]int:
		return "map[int]int"
	case map[int]int64:
		return "map[int]int64"
	case map[int]float32:
		return "map[int]float32"
	case map[int]float64:
		return "map[int]float64"

	case map[int64]string:
		return "map[int64]string"
	case map[int64]int:
		return "map[int64]int"
	case map[int64]int64:
		return "map[int64]int64"
	case map[int64]float32:
		return "map[int64]float32"
	case map[int64]float64:
		return "map[int64]float64"

	case map[float32]string:
		return "map[float32]string"
	case map[float32]int:
		return "map[float32]int"
	case map[float32]int64:
		return "map[float32]int64"
	case map[float32]float32:
		return "map[float32]float32"
	case map[float32]float64:
		return "map[float32]float64"

	case map[float64]string:
		return "map[float64]string"
	case map[float64]int:
		return "map[float64]int"
	case map[float64]int64:
		return "map[float64]int64"
	case map[float64]float32:
		return "map[float64]float32"
	case map[float64]float64:
		return "map[float64]float64"

	case []map[string]string:
		return "[]map[string]string"
	case []map[string]int:
		return "[]map[string]int"
	case []map[string]int64:
		return "[]map[string]int64"
	case []map[string]float32:
		return "[]map[string]float32"
	case []map[string]float64:
		return "[]map[string]float64"

	case []map[int]string:
		return "[]map[int]string"
	case []map[int]int:
		return "[]map[int]int"
	case []map[int]int64:
		return "[]map[int]int64"
	case []map[int]float32:
		return "[]map[int]float32"
	case []map[int]float64:
		return "[]map[int]float64"

	case []map[int64]string:
		return "[]map[int64]string"
	case []map[int64]int:
		return "[]map[int64]int"
	case []map[int64]int64:
		return "[]map[int64]int64"
	case []map[int64]float32:
		return "[]map[int64]float32"
	case []map[int64]float64:
		return "[]map[int64]float64"

	case []map[float32]string:
		return "[]map[float32]string"
	case []map[float32]int:
		return "[]map[float32]int"
	case []map[float32]int64:
		return "[]map[float32]int64"
	case []map[float32]float32:
		return "[]map[float32]float32"
	case []map[float32]float64:
		return "[]map[float32]float64"

	case []map[float64]string:
		return "[]map[float64]string"
	case []map[float64]int:
		return "[]map[float64]int"
	case []map[float64]int64:
		return "[]map[float64]int64"
	case []map[float64]float32:
		return "[]map[float64]float32"
	case []map[float64]float64:
		return "[]map[float64]float64"

	default:
		return ""
	}
}


