// Package leat is a git repository used as an append-only, per-author-lane
// message bus: an async, durable, cross-machine, audited transport behind a
// shared wire format.
//
// The one trick everything rests on is conflict-free by construction: each
// agent only ever appends to one file it owns — lanes/<id>.jsonl for DMs,
// channels/<chan>/<id>.jsonl for channel posts — so two agents' pushes never
// edit the same file and every push is a fast-forward. Sending is "append one
// JSON line + commit + push"; receiving is "fetch, read lanes past a
// reader-local cursor, filter to == me or chan in my subscriptions".
//
// leat is the canonical Go implementation of the mcp-dispatch git-transport
// wire contract (the language-independent interop seam). A consumer in another
// language interoperates by matching the on-disk JSONL envelope + lane layout,
// not this Go API.
package leat

import (
	"encoding/json"
	"fmt"
)

// WireVersion is the leat envelope schema version. Bump on any incompatible
// header change.
const WireVersion = 1

// Envelope is the shared wire record: one JSON object per line in a lane file.
// The header is all cleartext; only Body may be encrypted. Field order is not
// significant (any JSON parser reads by key), so this struct interoperates with
// the Python reference implementation without byte-identical serialization.
//
// Nullable fields (To, Chan, Key, TTL, Sig) use omitempty: an absent field is
// read as null by a lenient reader, so "to":null and an omitted "to" are
// equivalent on the wire. Seq and Version are never omitempty — seq 0 is a
// real first record, not "absent".
type Envelope struct {
	Type    string          `json:"type"`           // discriminator: message|atom|ack|presence|...
	From    string          `json:"from"`           // author id; must equal the lane owner
	To      string          `json:"to,omitempty"`   // DM recipient id (xor Chan)
	Chan    string          `json:"chan,omitempty"` // channel name, no leading '#' (xor To)
	Key     string          `json:"key,omitempty"`  // LWW partition; empty == event-stream record
	ID      string          `json:"id"`             // stable unique id; ref impl uses rec-<12hex>
	Ts      string          `json:"ts"`             // UTC, exactly 2006-01-02T15:04:05Z
	Seq     int             `json:"seq"`            // per-lane monotonic, 0-based
	TTL     int             `json:"ttl,omitempty"`  // seconds; 0 == never expire
	Version int             `json:"version"`        // == WireVersion
	Sig     string          `json:"sig,omitempty"`  // signature; reserved/unenforced in v1
	Body    json.RawMessage `json:"body"`           // opaque payload (encryptable)
}

// MarshalLine renders the envelope as a single JSONL line (no trailing newline;
// the caller adds it). encoding/json is compact by default.
func (e Envelope) MarshalLine() ([]byte, error) {
	if e.Version == 0 {
		e.Version = WireVersion
	}
	return json.Marshal(e)
}

// ParseLine parses one JSONL line into an Envelope, enforcing the two fields
// every record must carry.
func ParseLine(line []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(line, &e); err != nil {
		return Envelope{}, err
	}
	if e.Type == "" {
		return Envelope{}, fmt.Errorf("envelope missing required field: type")
	}
	if e.From == "" {
		return Envelope{}, fmt.Errorf("envelope missing required field: from")
	}
	return e, nil
}

// Body marshals an arbitrary value into an opaque body payload. Consumers
// (e.g. ettle, whose body is an atom slice) unmarshal it back themselves; leat
// never inspects body contents.
func Body(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

// MustBody is Body that panics on a marshal error — convenient for literals and
// tests where the value is statically known to be encodable.
func MustBody(v any) json.RawMessage {
	b, err := Body(v)
	if err != nil {
		panic(err)
	}
	return b
}
