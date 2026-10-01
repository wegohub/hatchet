package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
)

type memoryExternal struct {
	values  map[uuid.UUID][]byte
	loseACK bool
}

func (m *memoryExternal) Store(ctx context.Context, values ...repository.OffloadToExternalStoreOpts) (*repository.ExternalIndexFileLocationKey, error) {
	for _, v := range values {
		m.values[v.ExternalID] = append([]byte(nil), v.Payload...)
	}
	if m.loseACK {
		m.loseACK = false
		return nil, errors.New("external write acknowledgement lost")
	}
	key := repository.ExternalIndexFileLocationKey("index-file")
	return &key, nil
}
func (m *memoryExternal) Retrieve(ctx context.Context, opts ...repository.RetrieveFromExternalOpts) (map[repository.RetrieveFromExternalOpts][]byte, error) {
	out := make(map[repository.RetrieveFromExternalOpts][]byte)
	for _, opt := range opts {
		if opt.Method != repository.RetrieveFromExternalByIndexFile {
			return nil, errors.New("expected indexed retrieval")
		}
		out[opt] = m.values[opt.ByIndexFile.ExternalId]
	}
	return out, nil
}

type indexedPayloadStore struct {
	repository.PayloadStoreRepository
	external *memoryExternal
}

func (p *indexedPayloadStore) ExternalStoreEnabled() bool              { return true }
func (p *indexedPayloadStore) ExternalStore() repository.ExternalStore { return p.external }
func (p *indexedPayloadStore) RetrieveFromExternal(ctx context.Context, opts ...repository.RetrieveFromExternalOpts) (map[repository.RetrieveFromExternalOpts][]byte, error) {
	return p.external.Retrieve(ctx, opts...)
}
func TestPayloadCutoverRecoveryIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	external := &memoryExternal{values: make(map[uuid.UUID][]byte), loseACK: true}
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour, payloadStore: &indexedPayloadStore{external: external}}
	tenant, id := uuid.New(), uuid.New()
	at := timestamp(time.Now().Add(-48 * time.Hour))
	opt := repository.StoreOLAPPayloadOpts{ExternalId: id, InsertedAt: at, Payload: []byte(`{"value":42}`)}
	if err := r.PutPayloads(ctx, nil, tenant, opt); err != nil {
		t.Fatal(err)
	}
	opt.Payload = []byte(`{"value":99}`)
	if err := r.PutPayloads(ctx, nil, tenant, opt); err != nil {
		t.Fatal(err)
	}
	ttl := time.Hour
	if err := r.ProcessOLAPPayloadCutovers(ctx, true, &ttl, 100, 2, false); err == nil {
		t.Fatal("lost acknowledgement did not fail cutover")
	}
	p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: id, InsertedAt: at})
	if err != nil || string(p) != `{"value":42}` {
		t.Fatalf("inline payload lost before cutover commit %s %v", p, err)
	}
	if err = r.ProcessOLAPPayloadCutovers(ctx, true, &ttl, 100, 2, false); err != nil {
		t.Fatal(err)
	}
	p, err = r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: id, InsertedAt: at})
	if err != nil || string(p) != `{"value":42}` {
		t.Fatalf("indexed payload %s %v", p, err)
	}
	values, err := scanValues[storedPayload](ctx, r, entityFilter{Tenant: &tenant, Kind: "payload", ExternalIDs: []uuid.UUID{id}})
	if err != nil || len(values) != 1 || len(values[0].Inline) != 0 || !values[0].IndexFile {
		t.Fatalf("cutover location %#v %v", values, err)
	}
}
