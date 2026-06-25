# leat

*Of the keeping of records, and the avoidance of blots.*

Brother, attend. This is a scriptorium that wears the habit of a git repository:
an append-only, per-author message bus — async, durable, borne across machines,
and faithfully audited behind one shared form of writing. Two scribes on two
distant machines, schooled in two different tongues, hold converse by sharing a
single repository and reading one another's chronicles.

`leat` is the canonical Go hand that copies the mcp-dispatch git-transport wire
contract. The seam by which strangers interoperate is the on-disk JSONL form,
not this Go API — a brother writing in any language joins the order by matching
the bytes, not the binding.

## The Rule (the core idea)

Conflict-free by the discipline of the order: **each scribe appends only to the
one volume that bears his name, and to no other.**

- private letters: `lanes/<agent_id>.jsonl`
- public proclamations: `channels/<chan>/<agent_id>.jsonl`

Because no two hands ever touch one page, every offering to the shared library
is a fast-forward — no locks are forged, no merge driver is summoned, the
chronicle never blots nor needs the abbot's hand to reconcile. To **speak** is
to inscribe one line, seal it (commit), and bear it abroad (push). To **listen**
is to fetch the others' volumes and read past the ribbon you last left (a
reader-local cursor), heeding only what is addressed to you.

## The form of a line (wire format)

One JSON object to a line (JSONL), in honest UTF-8, closed by `\n`. The heading
is set down in plain hand for all to read; only the `body` may be enciphered.
The order of fields is of no consequence — any scribe reads by name.

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

The name upon the volume is the true name of its keeper. Should a page within
profess a `from` not its keeper's, it is a forgery, and we strike it on reading.

## The labor of the day (usage)

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

`Receive` is the tail read once and never again — the ribbon advances, no line
is delivered twice. `Collect` folds the chronicle to its latest word
(last-write-wins) and never disturbs the ribbon; set `Key` to a slot identity
for per-slot LWW. To read history proper — to trace how a single account changed
across its entries — take `bus.RepoDir()` with `bus.LaneRelPath(author, chan)`
and keep your own `git log`; the snapshot API does not wall the repository away,
by design.

## On forgery and trust (trust model)

A scribe's identity is the name upon his volume, and only that scribe may write
beneath it — that authority is granted by the git host's push ACLs, not by any
word within the record. A line professing a `from` at odds with the volume it
lies in is struck as a forgery on reading. Thus the boundary of safety is write
access to the repository: any brother who may push may append to *his own*
account, and none may counterfeit another's without first defeating the host's
own gatekeeping. `sig` is set aside for end-to-end signing but is not yet
enforced in v1; the heading is plain hand, and only the `body` may be enciphered.

## What this order is, and is not (scope)

A durable, audited, cross-boundary bus of measured pace — a chronicle, not a
crier in the square. Commit-and-push to the line is heavy by design; the worth
of it is durability and a permanent audit trail laid over the git host's
inherited auth and TLS. Encipherment is a seam left ready (the `body` is opaque),
yet sheathed by default in v1.

## License

MIT — see [LICENSE](LICENSE). *Go in peace.*
