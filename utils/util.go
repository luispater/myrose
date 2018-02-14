package utils

import (
	"fmt"
	"strings"
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

