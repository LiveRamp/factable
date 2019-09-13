// Package vtable runs the vtable.VTable consumer.
package main

import (
	"github.com/LiveRamp/factable/pkg/vtable"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(vtable.VTable)) }
