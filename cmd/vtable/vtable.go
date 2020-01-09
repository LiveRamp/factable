// Package vtable runs the vtable.VTable consumer.
package main

import (
	"github.com/LiveRamp/factable/pkg/vtable"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
)

func main() { runconsumer.Main(new(vtable.VTable)) }
