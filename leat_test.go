package leat

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// busPair builds a bare "server" repo plus two clones acting as two agents on
// two machines, all over a local path remote (no network).
func busPair(t *testing.T) (alice, bob *Bus) {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "bus.git")
	git(t, root, "init", "--bare", "-b", "main", bare)

	seed := filepath.Join(root, "seed")
	git(t, root, "clone", "-q", bare, seed)
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("leat bus\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, seed, "-c", "user.name=seed", "-c", "user.email=s@x", "add", "README.md")
	git(t, seed, "-c", "user.name=seed", "-c", "user.email=s@x", "commit", "-q", "-m", "seed")
	git(t, seed, "push", "-q", "origin", "main")

	aliceDir := filepath.Join(root, "alice")
	bobDir := filepath.Join(root, "bob")
	git(t, root, "clone", "-q", bare, aliceDir)
	git(t, root, "clone", "-q", bare, bobDir)

	var err error
	if alice, err = New(aliceDir, "alice", WithRemote("origin")); err != nil {
		t.Fatal(err)
	}
	if bob, err = New(bobDir, "bob", WithRemote("origin")); err != nil {
		t.Fatal(err)
	}
	return alice, bob
}

func pub(t *testing.T, b *Bus, env Envelope) {
	t.Helper()
	if _, err := b.Publish(context.Background(), env); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func recv(t *testing.T, b *Bus) []Envelope {
	t.Helper()
	got, err := b.Receive(context.Background())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return got
}

func bodyContent(t *testing.T, e Envelope) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(e.Body, &m); err != nil {
		t.Fatalf("body unmarshal: %v", err)
	}
	s, _ := m["content"].(string)
	return s
}

func TestEnvelopeRoundtrip(t *testing.T) {
	env := Envelope{Type: "message", From: "alice", To: "bob", Seq: 3, Body: MustBody(map[string]any{"content": "hi"})}
	line, err := env.MarshalLine()
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if again.From != "alice" || again.To != "bob" || again.Seq != 3 {
		t.Fatalf("roundtrip mismatch: %+v", again)
	}
	if again.Version != WireVersion {
		t.Fatalf("version: got %d want %d", again.Version, WireVersion)
	}
}

func TestEnvelopeGitFieldNames(t *testing.T) {
	line, _ := Envelope{Type: "message", From: "alice", To: "bob", Body: MustBody(map[string]any{})}.MarshalLine()
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(line, &keys); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"type", "from", "to", "id", "ts", "seq", "version", "body"} {
		if _, ok := keys[k]; !ok {
			t.Errorf("missing wire key %q in %s", k, line)
		}
	}
	if _, ok := keys["from_"]; ok {
		t.Error("must use git-native field name 'from', not 'from_'")
	}
}

func TestDMDelivery(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": "hello bob"})})
	got := recv(t, bob)
	if len(got) != 1 || got[0].From != "alice" || bodyContent(t, got[0]) != "hello bob" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestCursorNoRedelivery(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": "one"})})
	if got := recv(t, bob); len(got) != 1 {
		t.Fatalf("first receive: %+v", got)
	}
	if got := recv(t, bob); len(got) != 0 {
		t.Fatalf("second receive should be empty: %+v", got)
	}
	pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": "two"})})
	got := recv(t, bob)
	if len(got) != 1 || bodyContent(t, got[0]) != "two" {
		t.Fatalf("third receive: %+v", got)
	}
}

func TestFilteredNotForMe(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "carol", Body: MustBody(map[string]any{"content": "for carol"})})
	if got := recv(t, bob); len(got) != 0 {
		t.Fatalf("bob should get nothing: %+v", got)
	}
}

func TestOwnExcluded(t *testing.T) {
	alice, _ := busPair(t)
	pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": "to bob"})})
	if got := recv(t, alice); len(got) != 0 {
		t.Fatalf("alice should not receive her own: %+v", got)
	}
}

func TestChannelSubscription(t *testing.T) {
	alice, bob := busPair(t)
	_ = alice.Subscribe("general")
	_ = bob.Subscribe("general")
	pub(t, alice, Envelope{Chan: "general", Body: MustBody(map[string]any{"content": "hi all"})})
	if got := recv(t, bob); len(got) != 1 {
		t.Fatalf("subscriber should get it: %+v", got)
	}
	bob.Unsubscribe("general")
	pub(t, alice, Envelope{Chan: "general", Body: MustBody(map[string]any{"content": "still here"})})
	if got := recv(t, bob); len(got) != 0 {
		t.Fatalf("non-subscriber should get nothing: %+v", got)
	}
}

func TestSeqMonotonic(t *testing.T) {
	alice, bob := busPair(t)
	for _, c := range []string{"a", "b", "c"} {
		pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": c})})
	}
	got := recv(t, bob)
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	for i, e := range got {
		if e.Seq != i {
			t.Errorf("seq[%d] = %d, want %d", i, e.Seq, i)
		}
	}
}

func TestCollectLWWPerAuthor(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "bob", Type: "atom", Body: MustBody(map[string]any{"v": 1})})
	pub(t, alice, Envelope{To: "bob", Type: "atom", Body: MustBody(map[string]any{"v": 2})})
	snap, err := bob.Collect(context.Background(), CollectOptions{TypeFilter: "atom"})
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 1 {
		t.Fatalf("want 1 partition, got %d", len(snap))
	}
	var m map[string]any
	_ = json.Unmarshal(snap[0].Body, &m)
	if m["v"].(float64) != 2 {
		t.Fatalf("want latest v=2, got %v", m["v"])
	}
}

func TestCollectLWWPerKey(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "bob", Type: "atom", Key: "slot-x", Body: MustBody(map[string]any{"v": "x1"})})
	pub(t, alice, Envelope{To: "bob", Type: "atom", Key: "slot-y", Body: MustBody(map[string]any{"v": "y1"})})
	pub(t, alice, Envelope{To: "bob", Type: "atom", Key: "slot-x", Body: MustBody(map[string]any{"v": "x2"})})
	snap, err := bob.Collect(context.Background(), CollectOptions{TypeFilter: "atom"})
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]string{}
	for _, e := range snap {
		var m map[string]any
		_ = json.Unmarshal(e.Body, &m)
		byKey[e.Key] = m["v"].(string)
	}
	if byKey["slot-x"] != "x2" || byKey["slot-y"] != "y1" {
		t.Fatalf("per-key LWW wrong: %v", byKey)
	}
}

func TestConcurrentNoConflict(t *testing.T) {
	alice, bob := busPair(t)
	_ = alice.Subscribe("general")
	_ = bob.Subscribe("general")
	pub(t, alice, Envelope{Chan: "general", Body: MustBody(map[string]any{"content": "a1"})})
	pub(t, bob, Envelope{Chan: "general", Body: MustBody(map[string]any{"content": "b1"})})
	pub(t, alice, Envelope{Chan: "general", Body: MustBody(map[string]any{"content": "a2"})})
	pub(t, bob, Envelope{Chan: "general", Body: MustBody(map[string]any{"content": "b2"})})

	aliceIn := map[string]bool{}
	for _, e := range recv(t, alice) {
		aliceIn[bodyContent(t, e)] = true
	}
	if !aliceIn["b1"] || !aliceIn["b2"] || aliceIn["a1"] {
		t.Fatalf("alice saw wrong set: %v", aliceIn)
	}
	bobIn := map[string]bool{}
	for _, e := range recv(t, bob) {
		bobIn[bodyContent(t, e)] = true
	}
	if !bobIn["a1"] || !bobIn["a2"] || bobIn["b1"] {
		t.Fatalf("bob saw wrong set: %v", bobIn)
	}
}

func TestExactlyOneOfToOrChan(t *testing.T) {
	alice, _ := busPair(t)
	if _, err := alice.Publish(context.Background(), Envelope{Body: MustBody(map[string]any{})}); err == nil {
		t.Error("neither To nor Chan should error")
	}
	if _, err := alice.Publish(context.Background(), Envelope{To: "bob", Chan: "general", Body: MustBody(map[string]any{})}); err == nil {
		t.Error("both To and Chan should error")
	}
}

func TestCursorPersistsAcrossInstances(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": "persist me"})})
	if got := recv(t, bob); len(got) != 1 {
		t.Fatalf("first receive: %+v", got)
	}
	bob2, err := New(bob.RepoDir(), "bob", WithRemote("origin"))
	if err != nil {
		t.Fatal(err)
	}
	if got := recv(t, bob2); len(got) != 0 {
		t.Fatalf("fresh instance must not re-deliver: %+v", got)
	}
}

// TestIdentitySpoofGuard: a line in alice's lane claiming from=carol is dropped
// (filename identity is authoritative), and surfaced as a warning.
func TestIdentitySpoofGuard(t *testing.T) {
	alice, bob := busPair(t)
	pub(t, alice, Envelope{To: "bob", Body: MustBody(map[string]any{"content": "legit"})})

	// Forge a line into alice's lane claiming a different author, addressed to bob.
	forged := Envelope{Type: "message", From: "carol", To: "bob", ID: "rec-forged0000", Ts: "2026-01-01T00:00:00Z", Seq: 99, Version: WireVersion, Body: MustBody(map[string]any{"content": "spoofed"})}
	line, _ := forged.MarshalLine()
	lane := filepath.Join(alice.RepoDir(), "lanes", "alice.jsonl")
	f, err := os.OpenFile(lane, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write(append(line, '\n'))
	f.Close()
	git(t, alice.RepoDir(), "-c", "user.name=alice", "-c", "user.email=a@x", "commit", "-aqm", "forge")
	git(t, alice.RepoDir(), "push", "-q", "origin", "main")

	got := recv(t, bob)
	for _, e := range got {
		if bodyContent(t, e) == "spoofed" {
			t.Fatal("spoofed line from=carol in alice's lane should have been dropped")
		}
	}
	foundWarn := false
	for _, w := range bob.Warnings() {
		if len(w) > 0 {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Error("expected a warning for the dropped spoof line")
	}
}
