This Go implementation is a rigorous demonstration of how consistent hashing stabilizes a distributed system. Below is a deep dive into every functional block.

---

## 1. The Core Infrastructure

### The Hash Function (`hashKey`)
```go
func hashKey(key string) uint32 {
	digest := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(digest[:4])
}
```
* **What it does**: Maps any string (server name or data key) into a fixed $2^{32}$ integer space ($0$ to $4,294,967,295$).
* **Mechanism**: It computes an MD5 hash, takes the first 4 bytes, and converts them into a `uint32`.
* **Why MD5?**: While insecure for passwords, MD5 has an excellent "avalanche effect"—changing one character in the input results in a completely different output, ensuring keys spread evenly across the ring.

### The Ring Structure
```go
type Ring struct {
	vnodes    int
	positions []uint32
	owners    map[uint32]string
}
```
* **`vnodes`**: The number of virtual points per physical server (100 in this case).
* **`positions`**: A sorted slice of all virtual node locations on the ring. Sorting is critical for binary search.
* **`owners`**: A hash map that tells us: "If you land on coordinate $X$, the physical server responsible is $Y$."

---

## 2. Managing the Topology

### Adding Servers (`AddServer`)

```go
func (r *Ring) AddServer(name string) {
	for i := range r.vnodes {
		vnodeKey := fmt.Sprintf("%s#%d", name, i)
		pos := hashKey(vnodeKey)
		// ... collision check ...
		r.positions = append(r.positions, pos)
		r.owners[pos] = name
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}
```
* **Logic**: Instead of placing `Server_A` once, it places `Server_A#0`, `Server_A#1`... up to `Server_A#99`.
* **Virtual Nodes Benefit**: This prevents "hotspots." By interleaving 100 points per server, the ring is divided into 300-400 small segments rather than 3-4 massive ones.
* **Sorting**: After adding all points, the code re-sorts the `positions` slice. This ensures we can use binary search later.

---

## 3. Data Routing (`Lookup`)

```go
func (r *Ring) Lookup(key string) string {
	keyPos := hashKey(key)

	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= keyPos
	})

	if idx == len(r.positions) {
		idx = 0
	}
	return r.owners[r.positions[idx]]
}
```
1.  **Coordinate Find**: It hashes the incoming data key (e.g., "user_123") to find its point on the ring.
2.  **Binary Search**: `sort.Search` finds the first virtual node coordinate that is **greater than or equal to** the key's coordinate. This is the "clockwise successor."
3.  **The Wrap-Around**: If the key hashes to a value higher than any server (near the end of the $2^{32}$ range), `idx` will equal `len`. The code resets `idx = 0`, completing the "circle."

---

## 4. The Simulation & Proof (`main`)

### Phase 1: The Baseline
It routes **100,000 keys** to 3 servers (A, B, C). It records the result in a map `phase1[key] = server`. This establishes our "Before" state.

### Phase 2: Scale-Out
It creates a new ring with 4 servers (A, B, C, **D**) and routes the **exact same** 100,000 keys. It records this in `phase2`.

### The Mathematical Verification
```go
	for _, key := range keys {
		s1 := phase1[key]
		s2 := phase2[key]
		if s1 != s2 {
			totalMigrated++
		}
	}
```
* **Migration Check**: It compares every single key's location.
* **Consistent Hashing Property**: In a perfect implementation, keys should only move **to** the new server. No key should move from `Server_A` to `Server_B`.
* **The Result**: The code calculates the migration percentage. When moving from 3 to 4 servers, the new server owns $1/4$ of the ring. Therefore, only **~25%** of keys should migrate.

### The Contrast: Naive Modulo
Finally, the code runs a "Naive" test using `hash(key) % N`. 
* **N=3**: `key % 3`
* **N=4**: `key % 4`
* **Observation**: You will see that in the naive version, **~75%** of keys remap. This proves that consistent hashing is **3x more efficient** for cache stability in this scenario.

---

Would you like me to show you how to implement a **Replication Factor**, where each key is stored on the top 3 clockwise servers instead of just one for high availability?