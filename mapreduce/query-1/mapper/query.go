package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var targetCounty = "031"

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")
		if fields[1] == targetCounty {
			fmt.Printf("%s\t%d\n", targetCounty, 1)
		}
	}
}
