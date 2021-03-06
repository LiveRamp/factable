// Package quotes-extractor runs the extractor.Extractor consumer with `quote` package ExtractFns.
package main

import (
	"github.com/LiveRamp/factable/pkg/extractor"
	"github.com/LiveRamp/factable/pkg/testing/quotes"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(&extractor.Extractor{Extractors: quotes.BuildExtractors()}) }
