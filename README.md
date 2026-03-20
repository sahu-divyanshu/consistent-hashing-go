# Consistent Hashing Implementation

A production grade consistent hashing implementation written in Go. This repository demonstrates the programmatic solution to the catastrophic cache invalidation problem caused by standard modulo hashing.

## Architecture

1.  **Hash Space:** Both physical servers and data keys are mapped to a 32 bit circular integer space using the MD5 algorithm.
2.  **Virtual Nodes:** Each physical server is replicated 100 times around the ring to ensure statistically even data distribution and prevent data clustering.
3.  **Routing Algorithm:** Keys are routed to the first available clockwise server using a binary search (`sort.Search`) operating in O(log(N*V)) time.

## Mathematical Proof

The executable demonstrates a scale out event from 3 servers to 4 servers using a fixed dataset of 100,000 keys. 

According to consistent hashing principles, adding an Nth server requires exactly $1/N$ of the total keys to migrate to the new hardware. The code output mathematically proves that migrating from 3 to 4 servers results in approximately 25% key migration, with the remaining 75% safely mapped to their original locations. Standard modulo hashing would result in near 100% cache invalidation.

## Execution

Ensure you are running Go 1.26.0 or higher.

```bash
go run consistent_hash.go
