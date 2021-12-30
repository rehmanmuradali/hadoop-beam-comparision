package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rehmanmuradali/comparative-analysis-of-hadoop-apache-beam/mapreduce/utils"
)

var targetStateCode = "06"
var targetYear int64 = 2017

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		if fields[0] != targetStateCode {
			continue
		}
		if utils.GetYear(fields[11]) != targetYear {
			continue
		}

		fmt.Printf("%s\t%s\n", fields[1], fields[16])
	}
}
