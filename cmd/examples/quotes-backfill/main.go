// Package quotes-backfill runs backfill.Main with `quote` package ExtractFns.
package main

import (
	"github.com/LiveRamp/factable/pkg/backfill"
	"github.com/LiveRamp/factable/pkg/testing/quotes"
)

func main() { backfill.Main(quotes.BuildExtractors()) }
