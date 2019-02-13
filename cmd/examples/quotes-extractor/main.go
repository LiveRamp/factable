// Package quotes-extractor runs the extractor.Extractor consumer with `quote` package ExtractFns.
package main

import (
	"git.liveramp.net/jgraet/factable/pkg/extractor"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(&extractor.Extractor{Extractors: quotes.BuildExtractors()}) }
