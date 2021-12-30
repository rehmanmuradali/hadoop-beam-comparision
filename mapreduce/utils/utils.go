package utils

import (
	"log"
	"strconv"
)

func GetYear(date string) int64 {
	val, err := strconv.ParseInt(date[0:4], 10, 64)
	if err != nil {
		log.Fatalf("could not parse year timestamp. err = %v", err)
	}
	return val
}
