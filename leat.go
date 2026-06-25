package leat

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// idRe matches a safe single path segment: agent ids and channel names become
// lane filenames and directory names, so no dots, slashes, or leading dash.
var idRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$`)

func validID(kind, v string) error {
	if !idRe.MatchString(v) {
		return fmt.Errorf("invalid %s %q (must match %s)", kind, v, idRe.String())
	}
	return nil
}

// Bus is a git repo used as an append-only, per-author-lane message bus.
//
// repoDir is the working clone the agent reads/writes (its lane lives here).
// agentID is this agent's identity == its lane filename == commit author. When
// remote is set, Publish pushes and Receive/Collect fetch; with no remote the
// bus is local-only (still useful for tests and single-host).
type Bus struct {
	repoDir  string
	agentID  string
	remote   string
	branch   string
	stateDir string
	cur      *cursor

	mu       sync.Mutex
	subs     map[string]bool
	warnings []string
}

// Option configures a Bus.
type Option func(*Bus)

// WithRemote sets the git remote to push to / fetch from (e.g. "origin").
func WithRemote(remote string) Option { return func(b *Bus) { b.remote = remote } }

// WithStateDir overrides where the reader-local cursor is stored (default:
// <repoDir>/.git/leat).
func WithStateDir(dir string) Option { return func(b *Bus) { b.stateDir = dir } }

// New opens a Bus over an existing git working tree.
func New(repoDir, agentID string, opts ...Option) (*Bus, error) {
	if err := validID("agent_id", agentID); err != nil {
		return nil, err
	}
	b := &Bus{repoDir: repoDir, agentID: agentID, subs: map[string]bool{}}
	for _, o := range opts {
		o(b)
	}
	br, err := b.git(context.Background(), "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil || strings.TrimSpace(br) == "" || strings.TrimSpace(br) == "HEAD" {
		b.branch = "main"
	} else {
		b.branch = strings.TrimSpace(br)
	}
	if b.stateDir == "" {
		b.stateDir = filepath.Join(repoDir, ".git", "leat")
	}
	// Configure a local commit identity so commits AND rebases work even where
	// no global git identity is set (e.g. CI runners) — a missing identity makes
	// `pull --rebase` fail silently, stranding the local branch behind and
	// turning the next push into a non-fast-forward rejection.
	_, _ = b.git(context.Background(), "config", "user.name", agentID)
	_, _ = b.git(context.Background(), "config", "user.email", agentID+"@leat")
	b.cur = loadCursor(filepath.Join(b.stateDir, "cursor-"+agentID+".json"))
	return b, nil
}

// AgentID returns this bus's identity.
func (b *Bus) AgentID() string { return b.agentID }

// RepoDir returns the working-tree path, so a consumer (e.g. ettle's drift /
// provenance feature) can run its own `git log`/diff over a lane's history —
// the one capability the snapshot Publish/Collect API intentionally does not
// cover. leat deliberately does not encapsulate the repo away.
func (b *Bus) RepoDir() string { return b.repoDir }

// LaneRelPath returns the repo-relative POSIX path of an author's lane (the DM
// lane if chanName is empty, else that channel lane), suitable for
// `git -C RepoDir log -- <path>`.
func (b *Bus) LaneRelPath(author, chanName string) string {
	if chanName != "" {
		return b.relTo(b.chanLane(chanName, author))
	}
	return b.relTo(b.dmLane(author))
}

// -- git plumbing -----------------------------------------------------------

func (b *Bus) git(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = b.repoDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("git %s: %w: %s",
			strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func (b *Bus) relTo(abs string) string {
	r, err := filepath.Rel(b.repoDir, abs)
	if err != nil {
		return abs
	}
	return filepath.ToSlash(r)
}

// -- lane paths -------------------------------------------------------------

func (b *Bus) dmLane(id string) string {
	return filepath.Join(b.repoDir, "lanes", id+".jsonl")
}

func (b *Bus) chanLane(chanName, id string) string {
	return filepath.Join(b.repoDir, "channels", chanName, id+".jsonl")
}

func (b *Bus) myLane(chanName string) string {
	if chanName != "" {
		return b.chanLane(chanName, b.agentID)
	}
	return b.dmLane(b.agentID)
}

// laneOwner is the authoritative identity of a lane: its filename stem. A line
// inside the file claiming a different `from` is a spoof and is dropped on read
// (the DirBus filename-is-identity lesson, carried into git).
func laneOwner(path string) string {
	return strings.TrimSuffix(filepath.Base(path), ".jsonl")
}

func (b *Bus) laneFiles() []string {
	var out []string
	if dm, err := filepath.Glob(filepath.Join(b.repoDir, "lanes", "*.jsonl")); err == nil {
		sort.Strings(dm)
		out = append(out, dm...)
	}
	if ch, err := filepath.Glob(filepath.Join(b.repoDir, "channels", "*", "*.jsonl")); err == nil {
		sort.Strings(ch)
		out = append(out, ch...)
	}
	return out
}

// nextSeq derives the next per-lane sequence from the lane itself (count of
// records already written) so it can never drift from on-disk reality.
func (b *Bus) nextSeq(lane string) (int, error) {
	lines, err := readLines(lane)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, ln := range lines {
		if strings.TrimSpace(ln) != "" {
			n++
		}
	}
	return n, nil
}

// -- subscriptions ----------------------------------------------------------

// Subscribe adds a channel to this reader's filter (affects Receive only).
func (b *Bus) Subscribe(chanName string) error {
	if err := validID("channel", chanName); err != nil {
		return err
	}
	b.mu.Lock()
	b.subs[chanName] = true
	b.mu.Unlock()
	return nil
}

// Unsubscribe removes a channel from this reader's filter.
func (b *Bus) Unsubscribe(chanName string) {
	b.mu.Lock()
	delete(b.subs, chanName)
	b.mu.Unlock()
}

// -- publish ----------------------------------------------------------------

// Publish appends one record to this agent's own lane, commits it, and (if a
// remote is set) pushes. The caller fills Type, To/Chan, Key, and Body; leat
// fills From, ID, Ts, Seq, and Version and returns the finalized envelope.
// Exactly one of env.To (DM) or env.Chan (channel) must be set.
func (b *Bus) Publish(ctx context.Context, env Envelope) (Envelope, error) {
	if (env.To == "") == (env.Chan == "") {
		return env, fmt.Errorf("publish: exactly one of To or Chan must be set")
	}
	if env.To != "" {
		if err := validID("recipient", env.To); err != nil {
			return env, err
		}
	}
	if env.Chan != "" {
		if err := validID("channel", env.Chan); err != nil {
			return env, err
		}
	}
	if strings.ContainsAny(env.Key, "\r\n") {
		return env, fmt.Errorf("publish: key must not contain newlines")
	}
	if env.Type == "" {
		env.Type = "message"
	}
	if len(env.Body) == 0 {
		env.Body = json.RawMessage("null")
	}

	env.From = b.agentID
	lane := b.myLane(env.Chan)
	seq, err := b.nextSeq(lane)
	if err != nil {
		return env, err
	}
	env.Seq = seq
	env.ID = "rec-" + randHex(12)
	env.Ts = nowISO()
	env.Version = WireVersion

	line, err := env.MarshalLine()
	if err != nil {
		return env, err
	}
	if err := os.MkdirAll(filepath.Dir(lane), 0o755); err != nil {
		return env, err
	}
	f, err := os.OpenFile(lane, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return env, err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return env, err
	}
	if err := f.Close(); err != nil {
		return env, err
	}

	rel := b.relTo(lane)
	if _, err := b.git(ctx, "add", "--", rel); err != nil {
		return env, err
	}
	msg := fmt.Sprintf("%s %s from %s", env.Type, env.ID, env.From)
	if env.To != "" {
		msg += " to " + env.To
	} else {
		msg += " in #" + env.Chan
	}
	if _, err := b.git(ctx,
		"-c", "user.name="+b.agentID,
		"-c", "user.email="+b.agentID+"@leat",
		"commit", "-q", "-m", msg,
	); err != nil {
		return env, err
	}

	if b.remote != "" {
		if err := b.pushWithRebase(ctx); err != nil {
			return env, err
		}
	}
	return env, nil
}

// pushWithRebase publishes the local commit to the shared branch, rebasing onto
// any concurrent siblings' commits and retrying on a non-fast-forward push.
// Per-author lanes guarantee the rebase itself never hits a content conflict
// (no two agents edit the same file); the retry only handles the window between
// rebase and push where another writer's push lands first.
func (b *Bus) pushWithRebase(ctx context.Context) error {
	const maxTries = 6
	var pushErr error
	for try := 0; try < maxTries; try++ {
		_, _ = b.git(ctx, "pull", "--rebase", "-q", b.remote, b.branch)
		if _, pushErr = b.git(ctx, "push", "-q", b.remote, "HEAD:"+b.branch); pushErr == nil {
			return nil
		}
	}
	return fmt.Errorf("push to %s/%s failed after %d tries: %w", b.remote, b.branch, maxTries, pushErr)
}

// -- fetch ------------------------------------------------------------------

func (b *Bus) fetch(ctx context.Context) {
	if b.remote == "" {
		return
	}
	_, _ = b.git(ctx, "fetch", "-q", b.remote)
	_, _ = b.git(ctx, "merge", "-q", "--ff-only", b.remote+"/"+b.branch)
}

// -- receive (event-stream tail) --------------------------------------------

// Receive returns new records addressed to this agent since the last call,
// advancing and persisting the cursor. DMs where To == me and channel posts in
// subscribed channels are included; this agent's own records are excluded.
// Lines whose claimed From disagrees with the lane filename are dropped as
// spoofs (recorded in Warnings). Results are ordered by (ts, from, seq).
func (b *Bus) Receive(ctx context.Context) ([]Envelope, error) {
	b.fetch(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.warnings = nil

	var results []Envelope
	for _, lane := range b.laneFiles() {
		owner := laneOwner(lane)
		rel := b.relTo(lane)
		lines, err := readLines(lane)
		if err != nil {
			b.warn("unreadable lane %q: %v", rel, err)
			continue
		}
		start := b.cur.consumed[rel]
		consumed := start
		for i := start; i < len(lines); i++ {
			consumed = i + 1
			ln := strings.TrimSpace(lines[i])
			if ln == "" {
				continue
			}
			env, err := ParseLine([]byte(ln))
			if err != nil {
				b.warn("skip malformed line %d in %q: %v", i, rel, err)
				continue
			}
			if env.From != owner {
				b.warn("lane %q line %d claims from=%q; filename identity %q wins — dropped",
					rel, i, env.From, owner)
				continue
			}
			if env.From == b.agentID {
				continue
			}
			if env.Chan != "" {
				if b.subs[env.Chan] {
					results = append(results, env)
				}
			} else if env.To == b.agentID {
				results = append(results, env)
			}
		}
		b.cur.consumed[rel] = consumed
	}
	if err := b.cur.save(); err != nil {
		return results, err
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Ts != results[j].Ts {
			return results[i].Ts < results[j].Ts
		}
		if results[i].From != results[j].From {
			return results[i].From < results[j].From
		}
		return results[i].Seq < results[j].Seq
	})
	return results, nil
}

// -- collect (state snapshot) -----------------------------------------------

// CollectOptions tunes Collect.
type CollectOptions struct {
	// TypeFilter, if non-empty, restricts the snapshot to records of that type
	// (e.g. "atom").
	TypeFilter string
}

// Collect returns the latest record per (From, Key) across full lanes — the
// last-write-wins state-snapshot view. For event-stream records Key is empty,
// so the partition is per-author; state consumers set Key to a slot identity
// for per-slot LWW. Does not touch or advance the Receive cursor. Tie-break is
// (Seq, Ts). Spoofed lines (From != lane owner) are ignored.
func (b *Bus) Collect(ctx context.Context, opts CollectOptions) ([]Envelope, error) {
	b.fetch(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()

	type partition struct{ from, key string }
	latest := map[partition]Envelope{}
	for _, lane := range b.laneFiles() {
		owner := laneOwner(lane)
		lines, err := readLines(lane)
		if err != nil {
			continue
		}
		for _, raw := range lines {
			ln := strings.TrimSpace(raw)
			if ln == "" {
				continue
			}
			env, err := ParseLine([]byte(ln))
			if err != nil || env.From != owner {
				continue
			}
			if opts.TypeFilter != "" && env.Type != opts.TypeFilter {
				continue
			}
			p := partition{env.From, env.Key}
			cur, ok := latest[p]
			if !ok || env.Seq > cur.Seq || (env.Seq == cur.Seq && env.Ts > cur.Ts) {
				latest[p] = env
			}
		}
	}
	out := make([]Envelope, 0, len(latest))
	for _, e := range latest {
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// Warnings returns a copy of the non-fatal issues from the last Receive
// (skipped malformed lines, dropped identity spoofs, unreadable lanes).
func (b *Bus) Warnings() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.warnings) == 0 {
		return nil
	}
	return append([]string(nil), b.warnings...)
}

// Close releases resources. leat holds none; provided for transport-interface
// parity.
func (b *Bus) Close() error { return nil }

func (b *Bus) warn(format string, a ...any) {
	b.warnings = append(b.warnings, fmt.Sprintf(format, a...))
}

// -- small helpers ----------------------------------------------------------

func readLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	s := strings.TrimRight(string(data), "\n")
	if s == "" {
		return nil, nil
	}
	return strings.Split(s, "\n"), nil
}

func nowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

func randHex(nHex int) string {
	buf := make([]byte, (nHex+1)/2)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failure is unrecoverable; a non-unique id is worse than a
		// clear crash here.
		panic("leat: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(buf)[:nHex]
}
