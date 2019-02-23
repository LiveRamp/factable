package vtable

import (
	"sync/atomic"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"github.com/cockroachdb/cockroach/util/encoding"
	log "github.com/sirupsen/logrus"
)

type compactionFilter func() *factable.Schema

func (f compactionFilter) Filter(_ int, key, _ []byte) (remove bool, newVal []byte) {
	if key[0] == 0 {
		// Consumer metadata keys begin with \x00. VTable rows are >= \x01.
		return false, nil
	}

	key, mvTag, err := encoding.DecodeVarintAscending(key)
	if err != nil {
		log.WithField("err", err).Error("failed to determine view spec of row")
		return false, nil
	}

	var schema = f()
	mvSpec, ok := schema.Views[factable.MVTag(mvTag)]

	if !ok {
		// The MaterializedViewSpec for this row is no longer part of the Schema.
		// We remove the row, but first sanity-check that at least one
		// _other_ view remains. Eg, if the operator accidentally blows away the
		// Schema, we don't want to start dropping table data.
		return len(schema.Views) != 0, nil
	}
	if mvSpec.Retention == nil {
		return false, nil
	}

	var rowTime time.Time
	for _, tag := range mvSpec.ResolvedView.DimTags {
		if tag == mvSpec.Retention.RelativeToTag {
			_, rowTime, err = encoding.DecodeTimeAscending(key)

			if err == nil {
				// Did we exceed the RemoveAfter horizon of this row's encoded time?
				var horizon = rowTime.Add(mvSpec.Retention.RemoveAfter).Unix()
				return horizon < atomic.LoadInt64(approxUnixTime), nil
			}
		} else {
			key, err = schema.DequeDimension(key, tag)
		}

		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
				"dim": tag,
			}).Error("failed to deque dimension")

			return false, nil
		}
	}
	panic("not reached")
}

// Name of CompactionFilter. It cannot return a dynamically allocated
// string: this *must* be constant to be returned from cgo, which
// tecbot/gorocksdb will do. Null-terminate, because gorocksdb will directly
// hand this off as a c-string.
func (f compactionFilter) Name() string { return "vtable-compaction-filter\x00" }

// approxUnixTime is updated regularly with the Unix time,
// and must be accessed via atomic.LoadInt64
var approxUnixTime = new(int64)

// init approxUnixTime, and start a loop which updates it regularly.
func init() {
	atomic.StoreInt64(approxUnixTime, time.Now().Unix())

	go func(dst *int64) {
		for now := range time.NewTicker(time.Second).C {
			atomic.StoreInt64(dst, now.Unix())
		}
	}(approxUnixTime)
}
