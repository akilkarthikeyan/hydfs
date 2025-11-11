package main

import (
	"encoding/json"
	"time"
)

type Status string

const (
	Alive  Status = "alive"
	Failed Status = "failed"
)

type MessageType string

const (
	Gossip          MessageType = "gossip"
	JoinReq         MessageType = "join-req"
	JoinReply       MessageType = "join-reply"
	CreateHyDFSFile MessageType = "create-hydfs-file"
	AppendHyDFSFile MessageType = "append-hydfs-file"
	GetHyDFSFiles   MessageType = "get-hydfs-file"
	Merge           MessageType = "merge"
	MultiAppend     MessageType = "multi-append"
)

type ACKType string

const (
	ACK  ACKType = "ack"
	NACK ACKType = "nack"
)

type Member struct {
	// Gossip-related fields
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Timestamp   string `json:"timestamp"`
	RingID      uint64 `json:"ringId"`
	Heartbeat   int    `json:"heartbeat"`
	LastUpdated int    `json:"lastUpdated"`
	Status      Status `json:"status"`
}

type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Member         `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

type GossipPayload struct {
	Members map[string]Member `json:"Members"`
}

type File struct {
	Filename string `json:"filename"`
	DataB64  string `json:"dataB64"`
	ID       string `json:"id"` // chunk ID
}

type HyDFSFile struct {
	Filename               string
	HyDFSCompliantFilename string // without slashes
	Chunks                 []File // will contain DataB64 only during transport
}

type MergeRequest struct {
	HyDFSFilename string    `json:"hyDFSFilename"`
	MergeType     MergeType `json:"mergeType"`
}

type MultiAppendRequest struct {
	HyDFSFilename string `json:"hyDFSFilename"`
	LocalFilename string `json:"localFilename"`
}

type CreateHyDFSFileRequest struct {
	Chunk File `json:"file"`
}

type CreateHyDFSFileResponse struct {
	Ack ACKType `json:"ack"`
}

type AppendHyDFSFileRequest struct {
	Chunk File `json:"file"`
}

type AppendHyDFSFileResponse struct {
	Ack ACKType `json:"ack"`
}

type GetHyDFSFilesRequestType string

const (
	All     GetHyDFSFilesRequestType = "all"
	Primary GetHyDFSFilesRequestType = "primary"
	One     GetHyDFSFilesRequestType = "one"
	Meta    GetHyDFSFilesRequestType = "meta"
)

type MergeType string

const (
	MergeAll MergeType = "All"
	MergeOne MergeType = "One"
)

type GetHyDFSFilesRequest struct {
	Filename    string                   `json:"filename"`
	RequestType GetHyDFSFilesRequestType `json:"requestType"`
}

type GetHyDFSFilesResponse struct {
	Ack        ACKType              `json:"ack"`
	HyDFSFiles map[string]HyDFSFile `json:"hyDFSFiles,omitempty"`
}

const (
	SelfPort       = 1234
	IntroducerHost = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerPort = 1234
	Tfail          = 5
	Tcleanup       = 5
	Tmerge         = 30
	K              = 3
	TimeUnit       = time.Second
)
