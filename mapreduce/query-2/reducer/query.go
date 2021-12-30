package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

var targetStateCode = "06"
var targetYear int64 = 2017

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	sumCounts := make(map[string]float64)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		countyCode := fields[0]
		arithmeticMean, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			log.Fatalf("Could not parse mapper output. err = %v", err)
		}
		sumCounts[countyCode] = sumCounts[countyCode] + arithmeticMean
	}

	for countyCode, sum := range sumCounts {
		fmt.Printf("%s: %f\n", countyCode, sum)
	}
}
