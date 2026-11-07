package axonimport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ArchivedKeyPrefix names the disabled PicoTera keys standing in for AxonHub
// keys with no PicoTera counterpart, so imported rows can still be filtered
// by the client that sent them.
const ArchivedKeyPrefix = "axonhub/"

// Keys maps AxonHub api_keys.id to PicoTera api_key.id: explicit pairs
// first, otherwise a disabled archived key created on first use.
type Keys struct {
	UserID int64
	DB     *pgxpool.Pool // nil in dry-run; creation is then only simulated
	Map    map[int64]int32
	Names  map[int64]string // AxonHub key names

	mu      sync.Mutex
	nextID  int32
	Created atomic.Int64
}

func LoadAxonKeyNames(ctx context.Context, axon *pgxpool.Pool) (map[int64]string, error) {
	rows, err := axon.Query(ctx, `SELECT id, name FROM api_keys`)
	if err != nil {
		return nil, fmt.Errorf("list axonhub keys: %w", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[id] = name
	}
	return out, rows.Err()
}

// Resolve needs the PicoTera pool even in dry-run, to find archived keys an
// earlier run already created.
func (k *Keys) Resolve(ctx context.Context, pico *pgxpool.Pool, axonID *int64) (*int32, error) {
	if axonID == nil {
		return nil, nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if id, ok := k.Map[*axonID]; ok {
		return &id, nil
	}
	name, ok := k.Names[*axonID]
	if !ok {
		return nil, nil
	}
	name = ArchivedKeyPrefix + name
	var id int32
	err := pico.QueryRow(ctx, `SELECT id FROM api_key WHERE user_id = $1 AND name = $2 ORDER BY id LIMIT 1`, k.UserID, name).Scan(&id)
	switch {
	case err == nil:
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("look up key %q: %w", name, err)
	case k.DB == nil:
		k.nextID--
		id = k.nextID
		k.Created.Add(1)
	default:
		// A random, disabled key: it exists to label history, never to
		// authenticate.
		var b [24]byte
		_, _ = rand.Read(b[:])
		if err := k.DB.QueryRow(ctx,
			`INSERT INTO api_key (name, annotations, key, disabled, user_id) VALUES ($1, '{}', $2, true, $3) RETURNING id`,
			name, "axonhub-archived-"+hex.EncodeToString(b[:]), k.UserID).Scan(&id); err != nil {
			return nil, fmt.Errorf("create key %q: %w", name, err)
		}
		k.Created.Add(1)
	}
	if k.Map == nil {
		k.Map = map[int64]int32{}
	}
	k.Map[*axonID] = id
	return &id, nil
}
