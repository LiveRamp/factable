package internal

import (
	"errors"

	pb "go.gazette.dev/core/consumer/protocol"
)

// CommonConfig shared between the VTable and Extractor consumers.
type CommonConfig struct {
	Deltas         string `long:"deltas" description:"Journal label selector to which row delta events are published"`
	Instance       string `long:"instance" description:"Name of the Factable release instance"`
	SchemaKey      string `long:"schema" description:"Etcd key of shared SchemaSpec configuration"`
	TxnConcurrency uint   `long:"txnConcurrency" default:"0" description:"Concurrency of consumer transactions. 0 defaults to GOMAXPROCS."`
}

func (cfg CommonConfig) Validate() error {
	if cfg.Instance == "" {
		return errors.New("instance cannot be empty")
	} else if cfg.SchemaKey == "" {
		return errors.New("schema cannot be empty")
	} else if cfg.Deltas == "" {
		return errors.New("deltas cannot be empty")
	} else if _, err := pb.ParseLabelSelector(cfg.Deltas); err != nil {
		return err
	}
	return nil
}

func (cfg CommonConfig) DeltasSelector() pb.LabelSelector {
	if sel, err := pb.ParseLabelSelector(cfg.Deltas); err != nil {
		panic(err)
	} else {
		return sel
	}
}
