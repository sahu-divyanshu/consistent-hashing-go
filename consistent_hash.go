// consistent_hash.go
// ============================================================
// Production-grade Consistent Hashing with Virtual Nodes
// ============================================================
//
// Problem: modulo hashing  →  server_index = hash(key) % N
//   When N changes (add/remove server), almost every key remaps.
//   With N=3→4: ~75% of keys move. Catastrophic for a cache fleet.
//
// Solution: consistent hashing
//   Both servers and keys live on the same circular integer space
//   [0, 2^32). A key is owned by the first server clockwise from it
//   on the ring. When a server is added, only its immediate
//   counter-clockwise neighbours lose keys to it — everyone else
//   is unaffected.
//
// Virtual nodes: instead of placing each physical server once on
//   the ring, we place it V times (V=100 here). This distributes
//   the "landing zones" evenly so no single server inherits a
//   disproportionate arc when neighbours are removed.
//
// Binary search: the sorted slice of ring positions is searched
//   with sort.Search (equivalent to bisect_right in Python) to
//   find the clockwise successor in O(log(N*V)) time.
//
// Run:
//   go run consistent_hash.go
// ============================================================

package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Hash function
// ─────────────────────────────────────────────────────────────────────────────

// hashKey maps an arbitrary string to a position in [0, 2^32) using MD5.
// We take only the first 4 bytes of the 16-byte digest and interpret them
// as a big-endian uint32. MD5 is not cryptographically safe for security
// purposes, but its avalanche effect (small input change → large hash change)
// makes it excellent for even key distribution on a hash ring.
func hashKey(key string) uint32 {
	digest := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(digest[:4])
}

// ─────────────────────────────────────────────────────────────────────────────
// Ring data structure
// ─────────────────────────────────────────────────────────────────────────────

// Ring is the consistent hash ring.
//
// Fields:
//   vnodes     – how many virtual nodes to create per physical server
//   positions  – sorted slice of uint32 ring positions (length = servers * vnodes)
//   owners     – maps ring position → physical server name
//
// Invariant: len(positions) == len(owners) and positions is always sorted.
type Ring struct {
	vnodes    int
	positions []uint32
	owners    map[uint32]string
}

// NewRing constructs an empty ring with the given virtual node count.
func NewRing(vnodes int) *Ring {
	return &Ring{
		vnodes:  vnodes,
		owners:  make(map[uint32]string),
	}
}

// AddServer places a physical server onto the ring by hashing
// "serverName#vnode_index" for each virtual node index.
//
// After inserting all positions we re-sort the slice so binary search works.
// Re-sorting is O(V * log(N*V)) and only happens at topology change time,
// never on the hot path.
func (r *Ring) AddServer(name string) {
	for i := range r.vnodes {
		// Virtual node key: "Server_A#0", "Server_A#1", …, "Server_A#99"
		vnodeKey := fmt.Sprintf("%s#%d", name, i)
		pos := hashKey(vnodeKey)

		// Collision guard: if two vnode keys hash to the same position
		// (astronomically unlikely with uint32 and <1000 vnodes, but correct
		// to handle), nudge forward by 1.
		for {
			if _, exists := r.owners[pos]; !exists {
				break
			}
			pos++
		}

		r.positions = append(r.positions, pos)
		r.owners[pos] = name
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}

// Lookup returns the physical server responsible for key.
//
// Algorithm — clockwise successor search:
//   1. Hash the key to a uint32 position.
//   2. Binary-search the sorted positions slice for the first position
//      that is >= keyPos.
//   3. If no such position exists (keyPos > all ring positions), wrap
//      around to positions[0] — the ring is circular.
//
// Time complexity: O(log(N*V)) per lookup.
func (r *Ring) Lookup(key string) string {
	if len(r.positions) == 0 {
		return ""
	}
	keyPos := hashKey(key)

	// sort.Search returns the smallest index i such that positions[i] >= keyPos.
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= keyPos
	})

	// Wrap around: if keyPos is greater than every ring position,
	// the clockwise successor is positions[0].
	if idx == len(r.positions) {
		idx = 0
	}

	return r.owners[r.positions[idx]]
}

// Servers returns the distinct physical server names currently on the ring.
func (r *Ring) Servers() []string {
	seen := make(map[string]struct{})
	for _, name := range r.owners {
		seen[name] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ─────────────────────────────────────────────────────────────────────────────
// Key generation
// ─────────────────────────────────────────────────────────────────────────────

// generateKeys produces n random alphanumeric strings of length 12.
// A fixed seed is used so Phase 1 and Phase 2 operate on the identical
// key set — essential for a fair migration comparison.
func generateKeys(n int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	keys := make([]string, n)
	for i := range keys {
		b := make([]byte, 12)
		for j := range b {
			b[j] = charset[rng.Intn(len(charset))]
		}
		keys[i] = string(b)
	}
	return keys
}

// ─────────────────────────────────────────────────────────────────────────────
// Reporting helpers
// ─────────────────────────────────────────────────────────────────────────────

func printDistribution(label string, dist map[string]int, total int) {
	fmt.Printf("\n  %s\n", label)
	fmt.Printf("  %s\n", strings.Repeat("─", 48))
	fmt.Printf("  %-12s  %8s  %8s  %s\n", "Server", "Keys", "Percent", "Bar")
	fmt.Printf("  %s\n", strings.Repeat("─", 48))

	servers := make([]string, 0, len(dist))
	for s := range dist {
		servers = append(servers, s)
	}
	sort.Strings(servers)

	for _, s := range servers {
		count := dist[s]
		pct := float64(count) / float64(total) * 100
		bar := strings.Repeat("█", int(pct/2))
		fmt.Printf("  %-12s  %8d  %7.2f%%  %s\n", s, count, pct, bar)
	}
	fmt.Printf("  %s\n", strings.Repeat("─", 48))
	fmt.Printf("  %-12s  %8d  100.00%%\n\n", "TOTAL", total)
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	const (
		numKeys   = 100_000
		vnodes    = 100
		keySeed   = 42 // fixed seed → reproducible key set across both phases
	)

	start := time.Now()

	fmt.Println(strings.Repeat("═", 60))
	fmt.Println("  Consistent Hashing — Virtual Node Ring")
	fmt.Printf("  Keys: %d    VNodes per server: %d\n", numKeys, vnodes)
	fmt.Println(strings.Repeat("═", 60))

	// ── Generate the fixed key set once ──────────────────────────────────────
	keys := generateKeys(numKeys, keySeed)
	fmt.Printf("\n  Generated %d keys (seed=%d, deterministic)\n", numKeys, keySeed)

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 1 — Baseline ring with 3 servers
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("  PHASE 1 — Baseline: 3 servers (A, B, C)")
	fmt.Println(strings.Repeat("─", 60))

	ring3 := NewRing(vnodes)
	for _, srv := range []string{"Server_A", "Server_B", "Server_C"} {
		ring3.AddServer(srv)
		fmt.Printf("  [+] Added %-10s  (ring positions: %d)\n",
			srv, len(ring3.positions))
	}
	fmt.Printf("  Total ring positions: %d  (%d servers × %d vnodes)\n\n",
		len(ring3.positions), 3, vnodes)

	// Route all keys on the 3-server ring
	phase1 := make(map[string]string, numKeys) // key → server
	dist3 := make(map[string]int)
	for _, key := range keys {
		srv := ring3.Lookup(key)
		phase1[key] = srv
		dist3[srv]++
	}
	printDistribution("Phase 1 — Key Distribution (3 servers)", dist3, numKeys)

	// Standard deviation of distribution (lower = more even)
	mean3 := float64(numKeys) / 3.0
	var variance3 float64
	for _, c := range dist3 {
		d := float64(c) - mean3
		variance3 += d * d
	}
	stddev3 := math.Sqrt(variance3 / 3.0)
	fmt.Printf("  Distribution quality:\n")
	fmt.Printf("    Expected per server : %.0f keys\n", mean3)
	fmt.Printf("    Std deviation       : %.2f keys  (%.2f%% of mean)\n\n",
		stddev3, stddev3/mean3*100)

	// ─────────────────────────────────────────────────────────────────────────
	// Phase 2 — Scale-out: add 4th server
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("  PHASE 2 — Scale-out: add Server_D (now 4 servers)")
	fmt.Println(strings.Repeat("─", 60))

	// Clone the ring (re-add all servers) so Phase 1 ring is untouched
	ring4 := NewRing(vnodes)
	for _, srv := range []string{"Server_A", "Server_B", "Server_C", "Server_D"} {
		ring4.AddServer(srv)
		fmt.Printf("  [+] Added %-10s  (ring positions: %d)\n",
			srv, len(ring4.positions))
	}
	fmt.Printf("  Total ring positions: %d  (%d servers × %d vnodes)\n\n",
		len(ring4.positions), 4, vnodes)

	// Route all keys on the 4-server ring
	phase2 := make(map[string]string, numKeys)
	dist4 := make(map[string]int)
	for _, key := range keys {
		srv := ring4.Lookup(key)
		phase2[key] = srv
		dist4[srv]++
	}
	printDistribution("Phase 2 — Key Distribution (4 servers)", dist4, numKeys)

	// ─────────────────────────────────────────────────────────────────────────
	// Migration analysis
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("  MIGRATION ANALYSIS")
	fmt.Println(strings.Repeat("─", 60))

	// Per-server migration breakdown
	// A key "migrated" if its server assignment changed between phases.
	// With consistent hashing, keys can only migrate TO Server_D —
	// never between A, B, and C. We verify this property.
	type move struct{ from, to string }
	moveCounts := make(map[move]int)
	totalMigrated := 0

	for _, key := range keys {
		s1 := phase1[key]
		s2 := phase2[key]
		if s1 != s2 {
			moveCounts[move{s1, s2}]++
			totalMigrated++
		}
	}

	fmt.Printf("\n  Key movements (from → to):\n")
	// Collect and sort movement pairs for deterministic output
	type moveEntry struct {
		m     move
		count int
	}
	var moveList []moveEntry
	for m, c := range moveCounts {
		moveList = append(moveList, moveEntry{m, c})
	}
	sort.Slice(moveList, func(i, j int) bool {
		return moveList[i].m.from < moveList[j].m.from
	})
	for _, e := range moveList {
		fmt.Printf("    %-10s → %-10s  %d keys\n", e.m.from, e.m.to, e.count)
	}

	// Verify the consistent hashing property:
	// Keys that moved must have moved ONLY to the new server (Server_D).
	// If any key moved between existing servers, the implementation is wrong.
	illegalMoves := 0
	for m, c := range moveCounts {
		if m.to != "Server_D" {
			illegalMoves += c
			fmt.Printf("  ⚠  ILLEGAL MOVE detected: %s → %s (%d keys)\n",
				m.from, m.to, c)
		}
	}
	if illegalMoves == 0 {
		fmt.Printf("\n  ✅  Property verified: 0 keys moved between existing servers.\n")
		fmt.Printf("      All migration was exclusively to Server_D.\n")
	}

	// Mathematical proof
	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("  MATHEMATICAL PROOF")
	fmt.Println(strings.Repeat("─", 60))

	migratedPct := float64(totalMigrated) / float64(numKeys) * 100
	stayedPct   := float64(numKeys-totalMigrated) / float64(numKeys) * 100
	theoretical := 1.0 / 4.0 * 100 // 1/(N+1) = 1/4 = 25% when going from 3→4 servers

	fmt.Printf("\n  Theoretical expectation  : 1/(N+1) = 1/4 = %.2f%%\n", theoretical)
	fmt.Printf("    Derivation: the new server claims ~1/(N+1) of the ring arc.\n")
	fmt.Printf("    Keys uniformly distributed → ~1/(N+1) of keys should migrate.\n\n")

	fmt.Printf("  Observed results:\n")
	fmt.Printf("    Total keys             : %d\n", numKeys)
	fmt.Printf("    Keys migrated          : %d  (%.2f%%)\n", totalMigrated, migratedPct)
	fmt.Printf("    Keys stayed            : %d  (%.2f%%)\n", numKeys-totalMigrated, stayedPct)
	fmt.Printf("    Delta from theoretical : %.2f%%\n\n", math.Abs(migratedPct-theoretical))

	if math.Abs(migratedPct-theoretical) <= 3.0 {
		fmt.Printf("  ✅  Migration %.2f%% is within 3%% of the %.2f%% theoretical threshold.\n",
			migratedPct, theoretical)
	} else {
		fmt.Printf("  ⚠   Migration %.2f%% deviates >3%% from theoretical — increase vnodes.\n",
			migratedPct)
	}

	// Contrast with naive modulo hashing
	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("  COMPARISON: Naive Modulo Hashing vs Consistent Hashing")
	fmt.Println(strings.Repeat("─", 60))

	naiveMigrated := 0
	for _, key := range keys {
		pos := hashKey(key)
		old := pos % 3
		newP := pos % 4
		// Map numeric bucket to a name for comparison
		oldName := fmt.Sprintf("Server_%c", 'A'+old)
		newName := fmt.Sprintf("Server_%c", 'A'+newP)
		if oldName != newName {
			naiveMigrated++
		}
	}
	naivePct := float64(naiveMigrated) / float64(numKeys) * 100

	fmt.Printf("\n  Naive modulo (hash(k) %% N):\n")
	fmt.Printf("    Keys remapped when N 3→4 : %d  (%.2f%%)\n", naiveMigrated, naivePct)
	fmt.Printf("\n  Consistent hashing:\n")
	fmt.Printf("    Keys remapped when N 3→4 : %d  (%.2f%%)\n", totalMigrated, migratedPct)
	fmt.Printf("\n  Reduction in cache invalidation : %.1fx fewer moves\n",
		float64(naiveMigrated)/float64(totalMigrated))

	fmt.Printf("\n  Total elapsed: %v\n", time.Since(start))
	fmt.Println(strings.Repeat("═", 60))
}
