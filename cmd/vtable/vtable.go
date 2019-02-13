// Package vtable runs the vtable.VTable consumer.
package main

import (
	"git.liveramp.net/jgraet/factable/pkg/vtable"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(vtable.VTable)) }
