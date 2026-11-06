package axonimport

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/xid"
)

// PicoTera looks rows up by (id, created_at) and rebuilds created_at from the
// xid, so the xid must carry the row's own timestamp. Deriving the rest of the
// xid from the AxonHub id makes re-runs hit the same primary key, which is
// what keeps the import idempotent. The machine bytes "axh" keep these ids
// out of the space PicoTera's own xid.New() draws from.
var machine = [3]byte{'a', 'x', 'h'}

const (
	kindMeta     uint16 = 0
	kindUpstream uint16 = 1
)

func deterministicID(t time.Time, kind uint16, axonID int64) (xid.ID, error) {
	if axonID < 0 || axonID >= 1<<24 {
		return xid.NilID(), fmt.Errorf("axonhub id %d does not fit the 24-bit xid counter", axonID)
	}
	var b [12]byte
	binary.BigEndian.PutUint32(b[0:4], uint32(t.Unix()))
	copy(b[4:7], machine[:])
	binary.BigEndian.PutUint16(b[7:9], kind)
	b[9] = byte(axonID >> 16)
	b[10] = byte(axonID >> 8)
	b[11] = byte(axonID)
	return xid.FromBytes(b[:])
}
