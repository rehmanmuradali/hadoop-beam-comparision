package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

var targetCounty = "031"

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	var totalCountyCount int64
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		count, err := strconv.ParseInt(fields[1], 0, 10)
		if err != nil {
			log.Fatalf("Could not parse mapper output. err = %v", err)
		}
		totalCountyCount += count
	}
	fmt.Printf("%s: %d\n", targetCounty, totalCountyCount)
}
