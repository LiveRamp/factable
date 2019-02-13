// Package quotes-backfill runs backfill.Main with `quote` package ExtractFns.
package main

import (
	"git.liveramp.net/jgraet/factable/pkg/backfill"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
)

func main() { backfill.Main(quotes.BuildExtractors()) }
