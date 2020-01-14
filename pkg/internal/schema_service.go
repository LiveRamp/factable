package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.gazette.dev/core/keyspace"

	"github.com/LiveRamp/factable/pkg/factable"

	"go.etcd.io/etcd/clientv3"
)

// NewSchemaKeySpace returns a KeySpace over the singular shared SchemaSpec key.
func NewSchemaKeySpace(etcdPath string, fns *factable.ExtractFns) *keyspace.KeySpace {
	return keyspace.NewKeySpace(etcdPath,
		func(raw *mvccpb.KeyValue) (interface{}, error) {
			if string(raw.Key) != etcdPath {
				return nil, fmt.Errorf("unexpected key (%v; expected %v)", string(raw.Key), etcdPath)
			}

			var spec factable.SchemaSpec
			var schema = new(factable.Schema)

			if err := spec.Unmarshal(raw.Value); err != nil {
				return nil, err
			} else if *schema, err = factable.NewSchema(fns, spec); err != nil {
				return nil, err
			} else {
				return schema, nil
			}
		})
}

// SchemaService implements factable.SchemaServer, providing APIs over the shared SchemaSpec.
// It additionally provides a Schema accessor backed by a KeySpace.
type SchemaService struct {
	Config     CommonConfig
	KS         *keyspace.KeySpace
	Etcd       clientv3.KV
	ExtractFns *factable.ExtractFns
}

func (ss *SchemaService) Schema() (out *factable.Schema) {
	ss.KS.Mu.RLock()
	if len(ss.KS.KeyValues) == 1 {
		out = ss.KS.KeyValues[0].Decoded.(*factable.Schema)
	} else {
		out = new(factable.Schema)
	}
	ss.KS.Mu.RUnlock()
	return
}

func (ss *SchemaService) GetSchema(ctx context.Context, _ *empty.Empty) (*factable.GetSchemaResponse, error) {
	var resp = &factable.GetSchemaResponse{
		Instance:        ss.Config.Instance,
		DeltaPartitions: ss.Config.DeltasSelector(),
	}

	ss.KS.Mu.RLock()
	if len(ss.KS.KeyValues) == 1 {
		resp.ModRevision = ss.KS.KeyValues[0].Raw.ModRevision
		resp.Spec = ss.KS.KeyValues[0].Decoded.(*factable.Schema).Spec
	}
	ss.KS.Mu.RUnlock()

	return resp, nil
}

func (ss *SchemaService) UpdateSchema(ctx context.Context, req *factable.UpdateSchemaRequest) (*empty.Empty, error) {
	if next, err := factable.NewSchema(ss.ExtractFns, req.Update); err != nil {
		return nil, err
	} else if err = factable.ValidateSchemaTransition(*ss.Schema(), next); err != nil {
		return nil, err
	} else if ss.Config.Instance != req.ExpectInstance {
		return nil, fmt.Errorf("instance name mismatch (got %q, expected %q)", req.ExpectInstance, ss.Config.Instance)
	}

	// Non-nil ExtractFns provides stronger checks that the Schema is compatible
	// with compiled-in extractor types. Require that updates be directed to
	// Extractor pods, which are able to perform these deeper checks.
	if ss.ExtractFns == nil {
		return nil, errors.New("this is a VTable instance; schema updates must be directed to an Extractor")
	}

	var val, err = req.Update.Marshal()
	if err != nil {
		return nil, err
	}

	txn, err := ss.Etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(ss.KS.Root), "=", req.ExpectModRevision)).
		Then(clientv3.OpPut(ss.KS.Root, string(val))).
		Commit()

	if err != nil {
		return nil, err
	} else if !txn.Succeeded {
		return nil, fmt.Errorf("etcd transaction failed (wrong ExpectModRevision)")
	}

	ss.KS.Mu.RLock()
	defer ss.KS.Mu.RUnlock()

	if err = ss.KS.WaitForRevision(ctx, txn.Header.Revision); err != nil {
		return nil, err
	} else {
		return new(empty.Empty), nil
	}
}
