# HyDFS

HyDFS is a flat, quorum-replicated distributed file system written in Go.  
Files are chunked and replicated on 3 ring successors; writes require all 3 ACKs, reads need 1.  
No directories—filenames are treated as flat keys. Includes tools to inspect replicas and reconcile data after failures.

## Prerequisites

- Go 1.20+ installed on all VMs
- SSH access to all 10 course VMs  
  (`fa25-cs425-9501.cs.illinois.edu … fa25-cs425-9510.cs.illinois.edu`)

---

## Running

Run these steps on each VM (make sure introducer i.e VM1 is run first):

```bash
# 1. Clone this repo
git clone https://gitlab.engr.illinois.edu/akshatg4/g95.git g95
cd g95/mp3

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
| `ls <HyDFSfilename>` | List all VMs storing a given file and its RingID. |
| `liststore` | List files stored on this VM. |
| `getfromreplica <VMaddress> <HyDFSFilename> <localfilename>` | Get a file from a specific replica. |
| `multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilenamei> <localfilenamej> ...` | Simultaneously append from multiple VMs. |
