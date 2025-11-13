# HyDFS - test

## HyDFS Overview

HyDFS was built for MP3 at UIUC for CS425 Distributed Systems. HyDFS is a distributed file system built on top of a gossip-based failure detector (gossip round = 1s, K = 3, T<sub>fail</sub> = 5s, T<sub>cleanup</sub> = 5s). It uses **consistent hashing (SHA1, 64-bit)** to organize nodes in a ring of size `2^64` and has a **replication factor of 3** to ensure fault tolerance, allowing up to **two simultaneous node failures**.

### Communication & Storage
- **UDP** is used for control messages, while **TCP** (with Base64-encoded file data) is used to handle file transfers.  
- Each file is stored as **chunks**, each with a **unique ID** shared across replicas.  
- File data is stored under `/hydfs` in the local file system, and nodes maintain metadata about files and chunk ordering.

### Safety, re-replication and rebalancing
- **Write quorum = 3**, **read quorum = 1** ensures that correct data is always seen.  
- On **node join**, the new node fetches the files it should replicate from its successor.  
- On **node failure**, successors detect the failure and re-replicate the failed node’s files.

### Liveness
A **merge thread** runs every 30 seconds to:
- Enforce consistent chunk order across replicas (using the primary replica’s metadata).
- Create missing replicas if any are lost.
- Delete unnecessary replicas.

## Prerequisites

- Go 1.20+ installed on all machines
- Make sure to change introducer's address in *types.go* to your introducer's address

## Running

Run these steps on each machine (make sure introducer is run first):

```bash
# 1. Clone this repo
git clone https://github.com/akilkarthikeyan/hydfs.git hydfs
cd hydfs

# 2. Run the code as
go run .
```

## Commands

| Command | Usage |
|----------|--------|
| `list_mem_ids` | List all members with their RingIDs (sorted). |
| `list_self` | Print this node’s ID. |
| `create <localfilename> <HyDFSfilename>` | Create a new file on HyDFS from a local file. |
| `append <localfilename> <HyDFSfilename>` | Append a local file’s contents to an existing HyDFS file. |
| `get <HyDFSfilename> <localfilename>` | Fetch a file from HyDFS and save locally. |
| `merge <HyDFSfilename>` | Reconcile one file across replicas. |
| `ls <HyDFSfilename>` | List all nodes storing a given file and its RingID. |
| `liststore` | List files stored on this node. |
| `getfromreplica <nodeaddress> <HyDFSFilename> <localfilename>` | Get a file from a specific replica. |
| `multiappend <HyDFSfilename> <nodei> <nodej> ... <localfilenamei> <localfilenamej> ...` | Simultaneously append from multiple node. |






