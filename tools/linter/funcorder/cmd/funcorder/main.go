package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/uber/cadence/tools/linter/funcorder"
)

func main() {
	singlechecker.Main(funcorder.Analyzer)
}
