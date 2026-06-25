# leat

A git repository used as an append-only, per-author-lane message bus: an
async, durable, cross-machine, audited transport behind a shared wire format.
Two agents — on different machines, in different languages — interoperate by
sharing one git repo and reading each other's lanes.

`leat` is the canonical Go implementation of the mcp-dispatch git-transport
wire contract. The interop seam is the on-disk JSONL format, not this Go API,
so a consumer in any language interoperates by matching the bytes.

## The core idea

Conflict-free by construction. Each agent only ever appends to one file it
owns:

- DMs: `lanes/<agent_id>.jsonl`
- channel posts: `channels/<chan>/<agent_id>.jsonl`

Because no two agents write the same file, every push is a fast-forward — no
locks, no merge driver. Sending is *append one JSON line, commit, push*.
Receiving is *fetch, read lanes past a reader-local cursor, filter to records
addressed to me*.

## Wire format

One JSON object per line (JSONL), UTF-8, `\n`-terminated. The header is all
cleartext; only `body` may be encrypted. Field order is not significant.

| field | type | notes |
|-------|------|-------|
| `type` | string | record discriminator: `message`, `atom`, `ack`, … |
| `from` | string | author id; must equal the lane owner |
| `to` \| `chan` | string | DM recipient xor channel name (exactly one) |
| `key` | string | LWW partition; empty = event-stream record |
| `id` | string | stable unique id |
| `ts` | string | UTC, `2006-01-02T15:04:05Z` |
| `seq` | int | per-lane monotonic, 0-based |
| `ttl` | int | seconds; 0 = never expire |
| `version` | int | wire schema version |
| `sig` | string | signature; reserved/unenforced in v1 |
| `body` | any | opaque payload (encryptable) |

The lane filename is the authoritative identity: a line whose `from` disagrees
with its lane owner is dropped as a spoof.

## Usage

```go
bus, _ := leat.New(repoDir, "alice", leat.WithRemote("origin"))

// Send a DM and a channel post.
bus.Publish(ctx, leat.Envelope{To: "bob", Body: leat.MustBody(map[string]any{"content": "hi"})})
bus.Subscribe("general")
bus.Publish(ctx, leat.Envelope{Chan: "general", Body: leat.MustBody(map[string]any{"content": "standup"})})

// Event-stream view: new records addressed to me since last call.
msgs, _ := bus.Receive(ctx)

// State-snapshot view: latest record per (from, key).
snap, _ := bus.Collect(ctx, leat.CollectOptions{TypeFilter: "atom"})
```

`Receive` is the deliver-once tail (a cursor advances, no re-delivery).
`Collect` folds last-write-wins state and never touches the cursor — set
`Key` to a slot identity for per-slot LWW. For history (diff a lane's commits
over time), use `bus.RepoDir()` + `bus.LaneRelPath(author, chan)` and run your
own `git log`; the snapshot API intentionally does not encapsulate the repo
away.

## Scope

A durable, audited, cross-boundary, moderate-volume async bus — not a
real-time firehose. Per-message commit+push is heavy by design; the value is
durability and a permanent audit trail over inherited git-host auth/TLS.
Encryption is a designed seam (the `body` is opaque), default-off in v1.

## License

MIT — see [LICENSE](LICENSE).
