package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Tick int
var MembershipList sync.Map
var HyDFSFiles sync.Map

var selfHost string
var selfId string

var udpConn *net.UDPConn

func sendUDP(addr *net.UDPAddr, msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("send udp marshal error: %v", err)
		return
	}
	if _, err := udpConn.WriteToUDP(data, addr); err != nil {
		fmt.Printf("send udp write error: %v", err)
	}
}

func listenUDP() {
	buf := make([]byte, 4096)
	for {
		n, raddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("listen udp read error: %v", err)
			continue
		}
		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			fmt.Printf("listen udp unmarshal from %v error: %v", raddr, err)
			continue
		}
		handleMessage(&msg, nil)
	}
}

func sendTCP(addr string, msg *Message) (*Message, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Send request
	if err := encoder.Encode(msg); err != nil {
		return nil, err
	}

	// Wait for response
	var resp Message
	if err := decoder.Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func listenTCP(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("listen tcp accept error: %v", err)
			continue
		}
		// Handle each client in a separate goroutine
		go handleTCPClient(conn)
	}
}

func handleTCPClient(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				// Connection closed by client, so return
			} else {
				fmt.Printf("handle tcp decode from %v error: %v\n", conn.RemoteAddr(), err)
			}
			return
		}
		handleMessage(&msg, encoder)
	}
}

func mergeMembershipList(members map[string]Member) {
	for id, m := range members {
		v, ok := MembershipList.Load(id)
		if !ok { // New member
			newMember := Member{
				IP:          m.IP,
				Port:        m.Port,
				Timestamp:   m.Timestamp,
				RingID:      m.RingID,
				Heartbeat:   m.Heartbeat,
				LastUpdated: Tick,
				Status:      m.Status,
			}
			MembershipList.Store(id, newMember)
		} else { // Existing member
			existing := v.(Member)
			if m.Heartbeat > existing.Heartbeat {
				existing.Heartbeat = m.Heartbeat
				existing.Status = m.Status
				existing.LastUpdated = Tick
				MembershipList.Store(id, existing)
			}
		}
	}
}

func gossip(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		Tick++
		if (Tick % Tmerge) == 0 {
			go merge("", SnapshotMembers(true), MergeAll)
		}

		// Increment self heartbeat
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		self.Status = Alive
		self.Heartbeat++
		self.LastUpdated = Tick
		MembershipList.Store(selfId, self)

		// Check for failed/suspected members
		MembershipList.Range(func(k, v any) bool {
			m := v.(Member)
			elapsed := Tick - m.LastUpdated

			if m.Status == Alive && elapsed >= Tfail {
				m.Status = Failed
				m.LastUpdated = Tick
				MembershipList.Store(k.(string), m)
				// fmt.Printf("[FAIL] %s marked failed at tick %d\n", k.(string), tick)
				go handleNodeFail(m, SnapshotMembers(true))
			} else if m.Status == Failed && elapsed >= Tcleanup {
				MembershipList.Delete(k.(string))
				// fmt.Printf("[DELETE] %s removed from membership list at tick %d\n", k.(string), tick)
			}

			return true
		})

		// Select K random members to gossip to (exlude self)
		members := SnapshotMembers(true)
		delete(members, selfId)
		targets := SelectKMembers(members, K)
		members[selfId] = self // add self back

		// Gossip
		for _, target := range targets {
			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", target.IP, target.Port))
			if err != nil {
				fmt.Printf("gossip resolve target error: %v", err)
				continue
			}
			payloadBytes, _ := json.Marshal(GossipPayload{Members: members})
			msg := Message{
				MessageType: Gossip,
				From:        &self,
				Payload:     payloadBytes,
			}
			sendUDP(targetAddr, &msg)
			log.Printf("sent %s to %s", msg.MessageType, KeyFor(target))
		}
	}
}

func handleMessage(msg *Message, encoder *json.Encoder) { // encoder can only be present for TCP messages
	log.Printf("recv %s from %s", msg.MessageType, KeyFor(*msg.From))

	switch msg.MessageType {
	// UDP message
	case Gossip:
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)

	// UDP message
	case JoinReq:
		// Send JoinReply with current membership list
		members := SnapshotMembers(true)
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		payloadBytes, _ := json.Marshal(GossipPayload{Members: members})
		reply := Message{
			MessageType: JoinReply,
			From:        &self,
			Payload:     payloadBytes,
		}
		senderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.From.IP, msg.From.Port))
		if err != nil {
			fmt.Printf("resolve sender error: %v", err)
			return
		}
		sendUDP(senderAddr, &reply)
		log.Printf("sent %s to %s", reply.MessageType, KeyFor(*msg.From))

	// UDP message
	case JoinReply:
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)
		membershipList := SnapshotMembers(true)

		// Get all files from successor
		target := GetRingSuccessor(GetRingId(selfId), membershipList)
		files, err := getFilesFromTarget(target, "", All)
		if err != nil {
			fmt.Printf("get files from target error: %v", err)
			return
		}

		for filename, hyDFSFile := range files {
			// If successor is primary for file, skip copying it
			if KeyFor(GetRingSuccessor(GetRingId(filename), membershipList)) == KeyFor(target) {
				continue
			}
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}

			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}

	// UDP message
	case Merge:
		var mr MergeRequest
		if err := json.Unmarshal(msg.Payload, &mr); err != nil {
			fmt.Printf("merge request unmarshal error: %v", err)
			return
		}
		merge(mr.HyDFSFilename, SnapshotMembers(true), mr.MergeType)

	// UDP message
	case MultiAppend:
		var mar MultiAppendRequest
		if err := json.Unmarshal(msg.Payload, &mar); err != nil {
			fmt.Printf("multiappend request unmarshal error: %v", err)
			return
		}
		createOrAppendHyDFSFile(mar.LocalFilename, mar.HyDFSFilename, false)

	// TCP message
	case CreateHyDFSFile: // Returns ACK, or NACK if file already exists
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var chfr CreateHyDFSFileRequest
		if err := json.Unmarshal(msg.Payload, &chfr); err != nil {
			fmt.Printf("file payload unmarshal error: %v", err)
			return
		}

		// For the demo
		fmt.Printf("Received CreateHyDFSFile request for %s\n", chfr.Chunk.Filename)

		f := chfr.Chunk

		_, ok := HyDFSFiles.Load(f.Filename)
		if ok {
			payloadBytes, _ := json.Marshal(CreateHyDFSFileResponse{
				Ack: NACK,
			})
			encoder.Encode(&Message{
				MessageType: CreateHyDFSFile,
				From:        &self,
				Payload:     payloadBytes,
			})
			return
		}

		HyDFSFiles.Store(f.Filename, HyDFSFile{
			Filename:               f.Filename,
			HyDFSCompliantFilename: GetHyDFSCompliantFilename(f.Filename),
			Chunks: []File{
				{
					Filename: f.Filename,
					ID:       f.ID,
				},
			},
		})

		data, err := DecodeBase64ToBytes(f.DataB64)
		if err != nil {
			fmt.Printf("file decode error: %v", err)
			return
		}

		targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(f.Filename)+"_"+f.ID)
		if err := os.WriteFile(targetPath, data, 0644); err != nil {
			fmt.Printf("failed to write file: %v\n", err)
			return
		}

		payloadBytes, _ := json.Marshal(CreateHyDFSFileResponse{
			Ack: ACK,
		})
		encoder.Encode(&Message{
			MessageType: CreateHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})

		// For the demo
		fmt.Printf("CreateHyDFSFile operation for %s complete!\n", f.Filename)

	// TCP message
	case AppendHyDFSFile: // Returns ACK, or NACK if file does not exist
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var ahfr AppendHyDFSFileRequest
		if err := json.Unmarshal(msg.Payload, &ahfr); err != nil {
			fmt.Printf("file payload unmarshal error: %v", err)
			return
		}

		f := ahfr.Chunk

		// For the demo
		fmt.Printf("Received AppendHyDFSFile request for %s\n", f.Filename)

		w, ok := HyDFSFiles.Load(f.Filename)
		if !ok {
			payloadBytes, _ := json.Marshal(AppendHyDFSFileResponse{
				Ack: NACK,
			})
			encoder.Encode(&Message{
				MessageType: AppendHyDFSFile,
				From:        &self,
				Payload:     payloadBytes,
			})
			return
		}

		hyDFSFile := w.(HyDFSFile)
		hyDFSFile.Chunks = append(hyDFSFile.Chunks, File{
			Filename: f.Filename,
			ID:       f.ID,
		})
		HyDFSFiles.Store(f.Filename, hyDFSFile)

		data, err := DecodeBase64ToBytes(f.DataB64)
		if err != nil {
			fmt.Printf("file decode error: %v", err)
			return
		}

		targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(f.Filename)+"_"+f.ID)
		if err := os.WriteFile(targetPath, data, 0644); err != nil {
			fmt.Printf("failed to write file: %v\n", err)
			return
		}

		payloadBytes, _ := json.Marshal(AppendHyDFSFileResponse{
			Ack: ACK,
		})
		encoder.Encode(&Message{
			MessageType: AppendHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})

		// For the demo
		fmt.Printf("AppendHyDFSFile operation for %s complete!\n", f.Filename)

	// TCP message
	case GetHyDFSFiles: // Returns file payloads if file exists, else NACK; If "All", returns all files with an ACK, If "Primary", returns primary files only, If "Meta", returns metadata only
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var ghfr GetHyDFSFilesRequest
		if err := json.Unmarshal(msg.Payload, &ghfr); err != nil {
			fmt.Printf("get hydfs file payload unmarshal error: %v", err)
			return
		}

		switch ghfr.RequestType {
		case All:
			allFiles := make(map[string]HyDFSFile)
			HyDFSFiles.Range(func(k, v any) bool {
				filename := k.(string)
				hyDFSFile := v.(HyDFSFile)

				chunks := make([]File, 0)
				for _, chunk := range hyDFSFile.Chunks {
					targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(chunk.Filename)+"_"+chunk.ID)
					data, err := EncodeFileToBase64(targetPath)
					if err != nil {
						fmt.Printf("file encode error: %v", err)
						return false
					}
					chunks = append(chunks, File{
						Filename: chunk.Filename,
						DataB64:  data,
						ID:       chunk.ID,
					})
				}

				allFiles[filename] = HyDFSFile{
					Filename: filename,
					Chunks:   chunks,
				}
				return true
			})

			payloadBytes, _ := json.Marshal(GetHyDFSFilesResponse{
				Ack:        ACK,
				HyDFSFiles: allFiles,
			})
			encoder.Encode(&Message{
				MessageType: GetHyDFSFiles,
				From:        &self,
				Payload:     payloadBytes,
			})

		case Primary:
			primaryFiles := make(map[string]HyDFSFile)
			membershipList := SnapshotMembers(true)

			HyDFSFiles.Range(func(k, v any) bool {
				filename := k.(string)
				hyDFSFile := v.(HyDFSFile)
				if KeyFor(GetRingSuccessor(GetRingId(filename), membershipList)) != selfId {
					return true
				}

				chunks := make([]File, 0)
				for _, chunk := range hyDFSFile.Chunks {
					targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(chunk.Filename)+"_"+chunk.ID)
					data, err := EncodeFileToBase64(targetPath)
					if err != nil {
						fmt.Printf("file encode error: %v", err)
						return false
					}
					chunks = append(chunks, File{
						Filename: chunk.Filename,
						DataB64:  data,
						ID:       chunk.ID,
					})
				}

				primaryFiles[filename] = HyDFSFile{
					Filename: filename,
					Chunks:   chunks,
				}
				return true
			})

			payloadBytes, _ := json.Marshal(GetHyDFSFilesResponse{
				Ack:        ACK,
				HyDFSFiles: primaryFiles,
			})
			encoder.Encode(&Message{
				MessageType: GetHyDFSFiles,
				From:        &self,
				Payload:     payloadBytes,
			})

		case One:
			// For the demo
			fmt.Printf("Received GetHyDFSFiles request for %s\n", ghfr.Filename)

			filename := ghfr.Filename
			w, ok := HyDFSFiles.Load(filename)
			if !ok {
				payloadBytes, _ := json.Marshal(GetHyDFSFilesResponse{
					Ack: NACK,
				})
				encoder.Encode(&Message{
					MessageType: GetHyDFSFiles,
					From:        &self,
					Payload:     payloadBytes,
				})
				return
			}

			chunks := make([]File, 0)
			hyDFSFile := w.(HyDFSFile)
			for _, chunk := range hyDFSFile.Chunks {
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(chunk.Filename)+"_"+chunk.ID)
				data, err := EncodeFileToBase64(targetPath)
				if err != nil {
					fmt.Printf("file encode error: %v", err)
					return
				}
				chunks = append(chunks, File{
					Filename: chunk.Filename,
					DataB64:  data,
					ID:       chunk.ID,
				})
			}

			payloadBytes, _ := json.Marshal(GetHyDFSFilesResponse{
				Ack: ACK,
				HyDFSFiles: map[string]HyDFSFile{
					filename: {
						Filename: filename,
						Chunks:   chunks,
					},
				},
			})
			encoder.Encode(&Message{
				MessageType: GetHyDFSFiles,
				From:        &self,
				Payload:     payloadBytes,
			})

		case Meta:
			metaFiles := make(map[string]HyDFSFile)
			HyDFSFiles.Range(func(k, v any) bool {
				metaFiles[k.(string)] = v.(HyDFSFile)
				return true
			})

			payloadBytes, _ := json.Marshal(GetHyDFSFilesResponse{
				Ack:        ACK,
				HyDFSFiles: metaFiles,
			})
			encoder.Encode(&Message{
				MessageType: GetHyDFSFiles,
				From:        &self,
				Payload:     payloadBytes,
			})
		}
	}
}

func multicastMessage(msg *Message, members map[string]Member) {
	for _, target := range members {
		if KeyFor(target) == selfId {
			continue
		}
		targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", target.IP, target.Port))
		if err != nil {
			fmt.Printf("multicast resolve target error: %v", err)
			continue
		}
		sendUDP(targetAddr, msg)
	}
}

func createOrAppendHyDFSFile(localfilename string, hyDFSfilename string, create bool) bool {
	// Writes to a quorum of 3 replicas
	targets := make([]Member, 0, 3)
	membershipList := SnapshotMembers(true)

	targets = append(targets, GetRingSuccessor(GetRingId(hyDFSfilename), membershipList))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[0])), membershipList))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[1])), membershipList))

	fileContent, err := EncodeFileToBase64(localfilename)
	if err != nil {
		fmt.Printf("file encode error: %v", err)
		return false
	}

	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	var payloadBytes []byte

	if create {
		payloadBytes, _ = json.Marshal(CreateHyDFSFileRequest{
			Chunk: File{
				Filename: hyDFSfilename,
				DataB64:  fileContent,
				ID:       GetUUID(),
			},
		})
	} else {
		payloadBytes, _ = json.Marshal(AppendHyDFSFileRequest{
			Chunk: File{
				Filename: hyDFSfilename,
				DataB64:  fileContent,
				ID:       GetUUID(),
			},
		})
	}

	var msgType MessageType
	if create {
		msgType = CreateHyDFSFile
	} else {
		msgType = AppendHyDFSFile
	}

	message := Message{
		MessageType: msgType,
		From:        &self,
		Payload:     payloadBytes,
	}

	var wg sync.WaitGroup
	results := make(chan *Message, len(targets))
	wg.Add(len(targets))

	for _, t := range targets {
		go func() {
			defer wg.Done()
			addr := fmt.Sprintf("%s:%d", t.IP, t.Port)
			resp, err := sendTCP(addr, &message)
			if err != nil {
				// fmt.Printf("send to %s failed: %v\n", addr, err)
				return
			}
			results <- resp
		}()
	}

	wg.Wait()
	close(results)

	var all []*Message
	for r := range results {
		all = append(all, r)
	}

	successes := 0
	if create {
		for _, message := range all {
			var createHyDFSFileResponse CreateHyDFSFileResponse
			if err := json.Unmarshal(message.Payload, &createHyDFSFileResponse); err != nil {
				fmt.Printf("ack unmarshal error: %v", err)
				continue
			}
			if createHyDFSFileResponse.Ack == ACK {
				successes++
			}
		}
	} else {
		for _, message := range all {
			var appendHyDFSFileResponse AppendHyDFSFileResponse
			if err := json.Unmarshal(message.Payload, &appendHyDFSFileResponse); err != nil {
				fmt.Printf("ack unmarshal error: %v", err)
				continue
			}
			if appendHyDFSFileResponse.Ack == ACK {
				successes++
			}
		}
	}

	return successes == 3
}

func getHyDFSFile(hyDFSfilename string, localfilename string) bool {
	// Read, but read quorum is just 1
	targets := make([]Member, 0, 3)
	membershipList := SnapshotMembers(true)

	targets = append(targets, GetRingSuccessor(GetRingId(hyDFSfilename), membershipList))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[0])), membershipList))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[1])), membershipList))

	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	payloadBytes, _ := json.Marshal(GetHyDFSFilesRequest{
		Filename:    hyDFSfilename,
		RequestType: One,
	})
	req := Message{
		MessageType: GetHyDFSFiles,
		From:        &self,
		Payload:     payloadBytes,
	}

	result := make(chan GetHyDFSFilesResponse, len(targets))

	for _, t := range targets {
		go func() {
			addr := fmt.Sprintf("%s:%d", t.IP, t.Port)
			resp, err := sendTCP(addr, &req)
			if err != nil {
				result <- GetHyDFSFilesResponse{
					Ack: NACK,
				}
				return
			}

			var ghfr GetHyDFSFilesResponse
			if err := json.Unmarshal(resp.Payload, &ghfr); err != nil {
				result <- GetHyDFSFilesResponse{
					Ack: NACK,
				}
				return
			}

			result <- ghfr
		}()
	}

	// Consume up to len(targets) replies in completion order; stop on first ACK
	for i := 0; i < len(targets); i++ {
		ghfr := <-result
		if ghfr.Ack == ACK {
			var full []byte
			for _, fp := range ghfr.HyDFSFiles[hyDFSfilename].Chunks {
				data, err := DecodeBase64ToBytes(fp.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return false
				}
				full = append(full, data...)
			}

			if err := os.WriteFile(localfilename, full, 0644); err != nil {
				fmt.Printf("failed to write file: %v\n", err)
				return false
			}
			return true
		}
	}

	return false
}

func getFilesFromTarget(target Member, hyDFSfilename string, requestType GetHyDFSFilesRequestType) (map[string]HyDFSFile, error) {
	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	payloadBytes, _ := json.Marshal(GetHyDFSFilesRequest{
		Filename:    hyDFSfilename,
		RequestType: requestType,
	})
	req := Message{
		MessageType: GetHyDFSFiles,
		From:        &self,
		Payload:     payloadBytes,
	}

	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	resp, err := sendTCP(addr, &req)
	if err != nil {
		return nil, err
	}

	var ghfr GetHyDFSFilesResponse
	if err := json.Unmarshal(resp.Payload, &ghfr); err != nil {
		return nil, err
	}

	return ghfr.HyDFSFiles, nil
}

// Redistributes replicas
func handleNodeFail(m Member, membershipList map[string]Member) {
	// Take responsibilty for replicas that failed node replicated if you are a successor or 2nd successor
	// Take responsibilty for replicas that failed node was primary for if you are 3rd successor (first 2 successors already replicate it)
	successor := GetRingSuccessor(GetRingId(KeyFor(m)), membershipList)
	successor2 := GetRingSuccessor(GetRingId(KeyFor(successor)), membershipList)
	successor3 := GetRingSuccessor(GetRingId(KeyFor(successor2)), membershipList)
	predecessor := GetRingPredecessor(GetRingId(selfId), membershipList)
	predecessor2 := GetRingPredecessor(GetRingId(KeyFor(predecessor)), membershipList)

	if KeyFor(successor) == selfId {
		// Get primary files from predecessor (this should technically already be in self)
		files, err := getFilesFromTarget(predecessor, "", Primary)
		if err != nil {
			fmt.Printf("get files from target error: %v", err)
			return
		}

		for filename, hyDFSFile := range files {
			_, ok := HyDFSFiles.Load(filename)
			if ok {
				continue
			}
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}

		// Get primary files from predecessor2
		files2, err := getFilesFromTarget(predecessor2, "", Primary)
		if err != nil {
			fmt.Printf("get files from target error: %v", err)
			return
		}

		for filename, hyDFSFile := range files2 {
			_, ok := HyDFSFiles.Load(filename)
			if ok {
				continue
			}
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}

	} else if KeyFor(successor2) == selfId {
		// Get primary files from predecessor2
		files2, err := getFilesFromTarget(predecessor2, "", Primary)
		if err != nil {
			fmt.Printf("get files from target error: %v", err)
			return
		}

		for filename, hyDFSFile := range files2 {
			_, ok := HyDFSFiles.Load(filename)
			if ok {
				continue
			}
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}

	} else if KeyFor(successor3) == selfId {
		// Get all files from successor
		files, err := getFilesFromTarget(successor, "", All)
		if err != nil {
			fmt.Printf("get files from target error: %v", err)
			return
		}

		for filename, hyDFSFile := range files {
			// If successor is not primary for file, skip copying it
			if KeyFor(GetRingSuccessor(GetRingId(filename), membershipList)) != KeyFor(successor) {
				continue
			}
			_, ok := HyDFSFiles.Load(filename)
			// Skip if already have it
			if ok {
				continue
			}
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}
	} else {
		return
	}
}

// Ensures order of chunks is the same as primary, creates missing files, and deletes extraneous files
func merge(hyDFSFile string, membershipList map[string]Member, mergeType MergeType) {
	predecessor := GetRingPredecessor(GetRingId(selfId), membershipList)
	predecessor2 := GetRingPredecessor(GetRingId(KeyFor(predecessor)), membershipList)

	metaFiles, err := getFilesFromTarget(predecessor, "", Meta)
	if err != nil {
		fmt.Printf("get files from target error: %v", err)
		return
	}
	metaFiles2, err := getFilesFromTarget(predecessor2, "", Meta)
	if err != nil {
		fmt.Printf("get files from target error: %v", err)
		return
	}

	// Delete non-primary files
	for filename := range metaFiles {
		if KeyFor(GetRingSuccessor(GetRingId(filename), membershipList)) != KeyFor(predecessor) {
			delete(metaFiles, filename)
		}
	}
	for filename := range metaFiles2 {
		if KeyFor(GetRingSuccessor(GetRingId(filename), membershipList)) != KeyFor(predecessor2) {
			delete(metaFiles2, filename)
		}
	}

	switch mergeType {
	case MergeAll:
		HyDFSFiles.Range(func(k, v any) bool {
			filename := k.(string)
			if primaryCopy, ok := metaFiles[filename]; ok {
				myCopy := v.(HyDFSFile)
				myCopy.Chunks = primaryCopy.Chunks
				HyDFSFiles.Store(filename, myCopy)
			} else if primaryCopy, ok := metaFiles2[filename]; ok {
				myCopy := v.(HyDFSFile)
				myCopy.Chunks = primaryCopy.Chunks
				HyDFSFiles.Store(filename, myCopy)
			} else if KeyFor(GetRingSuccessor(GetRingId(filename), membershipList)) != selfId {
				// Delete this file from local file system and HyDFSFiles
				myCopy := v.(HyDFSFile)
				for _, chunk := range myCopy.Chunks {
					targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(chunk.Filename)+"_"+chunk.ID)
					os.Remove(targetPath)
				}
				HyDFSFiles.Delete(filename)
			}
			return true
		})
		// Get any missing primary files
		for filename := range metaFiles {
			_, ok := HyDFSFiles.Load(filename)
			if ok {
				continue
			}
			hyDFSFiles, err := getFilesFromTarget(predecessor, filename, One)
			if err != nil {
				fmt.Printf("get files from target error: %v", err)
				return
			}
			hyDFSFile := hyDFSFiles[filename]
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}
		for filename := range metaFiles2 {
			_, ok := HyDFSFiles.Load(filename)
			if ok {
				continue
			}
			hyDFSFiles, err := getFilesFromTarget(predecessor2, filename, One)
			if err != nil {
				fmt.Printf("get files from target error: %v", err)
				return
			}
			hyDFSFile := hyDFSFiles[filename]
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(filename, HyDFSFile{
				Filename:               filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(filename),
				Chunks:                 chunks,
			})
		}

	case MergeOne:
		inMeta := true
		primaryCopy, ok := metaFiles[hyDFSFile]
		if !ok {
			primaryCopy, ok = metaFiles2[hyDFSFile]
			if !ok {
				return
			}
			inMeta = false
		}
		v, ok := HyDFSFiles.Load(hyDFSFile)
		if !ok {
			var (
				hyDFSFiles map[string]HyDFSFile
				err        error
			)
			if inMeta {
				hyDFSFiles, err = getFilesFromTarget(predecessor, hyDFSFile, One)
				if err != nil {
					fmt.Printf("get files from target error: %v", err)
					return
				}
			} else {
				hyDFSFiles, err = getFilesFromTarget(predecessor2, hyDFSFile, One)
				if err != nil {
					fmt.Printf("get files from target error: %v", err)
					return
				}
			}
			hyDFSFile := hyDFSFiles[hyDFSFile]
			chunks := make([]File, 0)
			for _, chunk := range hyDFSFile.Chunks {
				data, err := DecodeBase64ToBytes(chunk.DataB64)
				if err != nil {
					fmt.Printf("file decode error: %v", err)
					return
				}
				targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(hyDFSFile.Filename)+"_"+chunk.ID)
				if err := os.WriteFile(targetPath, data, 0644); err != nil {
					fmt.Printf("failed to write file: %v\n", err)
					return
				}
				chunks = append(chunks, File{
					Filename: hyDFSFile.Filename,
					ID:       chunk.ID,
				})
			}
			HyDFSFiles.Store(hyDFSFile.Filename, HyDFSFile{
				Filename:               hyDFSFile.Filename,
				HyDFSCompliantFilename: GetHyDFSCompliantFilename(hyDFSFile.Filename),
				Chunks:                 chunks,
			})
		} else {
			myCopy := v.(HyDFSFile)
			myCopy.Chunks = primaryCopy.Chunks
			HyDFSFiles.Store(hyDFSFile, myCopy)
		}
	}
}

func main() {
	f, err := os.OpenFile("machine.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("log file open error: %v", err)
		return
	}
	defer f.Close()
	log.SetOutput(f)

	// Erase previous hydfs files and make dir
	os.RemoveAll("hydfs")
	os.Mkdir("hydfs", 0755)

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("get hostname error: %v", err)
		return
	}
	selfHost = hostname

	// Add self to membership list
	self := Member{
		IP:        selfHost,
		Port:      SelfPort,
		Timestamp: GetUUID(), // unique
		Heartbeat: 0,
		Status:    Alive,
	}
	selfId = KeyFor(self)
	self.RingID = GetRingId(KeyFor(self))
	MembershipList.Store(selfId, self)

	// Listen for UDP messages
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("resolve listenAddr error: %v", err)
		return
	}
	udpConn, err = net.ListenUDP("udp", listenAddr)
	if err != nil {
		fmt.Printf("udp error: %v", err)
	}
	defer udpConn.Close()

	go listenUDP()

	// Listen for TCP messages
	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("tcp error: %v", err)
		return
	}

	go listenTCP(tcpLn)

	// Send join to introducer iff we are not the introducer
	if !(selfHost == IntroducerHost && SelfPort == IntroducerPort) {
		introducerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", IntroducerHost, IntroducerPort))
		if err != nil {
			fmt.Printf("resolve introducerAddr error: %v", err)
		}
		initial := Message{
			MessageType: JoinReq,
			From:        &self,
		}
		sendUDP(introducerAddr, &initial)
		log.Printf("sent %s to %s:%d", initial.MessageType, IntroducerHost, IntroducerPort)
	}

	go gossip(TimeUnit)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("stdin error: %v\n", err)
	}
}

func handleCommand(line string) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}

	switch fields[0] {
	case "list_mem_ids":
		members := SnapshotMembers(false)

		type kv struct {
			ID string
			M  Member
		}

		var sorted []kv
		for id, m := range members {
			sorted = append(sorted, kv{id, m})
		}

		// Sort by RingID
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].M.RingID < sorted[j].M.RingID
		})

		fmt.Printf("Membership List (sorted by RingID):\n")
		for _, kv := range sorted {
			m := kv.M
			fmt.Printf("RingID: %20d | ID: %s | Status: %s | Heartbeat: %d | LastUpdated: %d\n",
				m.RingID, kv.ID, m.Status, m.Heartbeat, m.LastUpdated)
		}

	case "list_self":
		fmt.Println(selfId)

	case "create":
		if len(fields) != 3 {
			fmt.Println("Usage: create <localfilename> <HyDFSfilename>")
			return
		}
		localfilename := fields[1]
		hyDFSfilename := fields[2]
		success := createOrAppendHyDFSFile(localfilename, hyDFSfilename, true)
		if success {
			fmt.Printf("%s written to HyDFS!\n", hyDFSfilename)
		} else {
			fmt.Printf("Failed to write to HyDFS\n")
		}

	case "append":
		if len(fields) != 3 {
			fmt.Println("Usage: append <localfilename> <HyDFSfilename>")
			return
		}
		localfilename := fields[1]
		hyDFSfilename := fields[2]
		success := createOrAppendHyDFSFile(localfilename, hyDFSfilename, false)
		if success {
			fmt.Printf("Appended to HyDFS file %s!\n", hyDFSfilename)
		} else {
			fmt.Printf("Failed to append to HyDFS\n")
		}

	case "get":
		if len(fields) != 3 {
			fmt.Println("Usage: get <HyDFSfilename> <localfilename>")
			return
		}
		hyDFSfilename := fields[1]
		localfilename := fields[2]
		success := getHyDFSFile(hyDFSfilename, localfilename)
		if success {
			fmt.Printf("Retrieved HyDFS file %s to %s!\n", hyDFSfilename, localfilename)
		} else {
			fmt.Printf("Failed to retrieve HyDFS file\n")
		}

	case "merge":
		if len(fields) != 2 {
			fmt.Println("Usage: merge <HyDFSfilename>")
			return
		}
		hyDFSfilename := fields[1]
		membershipList := SnapshotMembers(true)
		merge(hyDFSfilename, membershipList, MergeOne)

		v, _ := MembershipList.Load(selfId)
		self := v.(Member)
		payloadBytes, _ := json.Marshal(MergeRequest{
			HyDFSFilename: hyDFSfilename,
			MergeType:     MergeOne,
		})
		msg := Message{
			MessageType: Merge,
			From:        &self,
			Payload:     payloadBytes,
		}
		multicastMessage(&msg, membershipList)

		fmt.Printf("Merge for %s initiated!\n", hyDFSfilename)

	case "ls":
		if len(fields) != 2 {
			fmt.Println("Usage: ls <HyDFSfilename>")
			return
		}
		hyDFSfilename := fields[1]
		membershipList := SnapshotMembers(true)
		isPresent := false

		for id, target := range membershipList {
			hyDFSFiles, err := getFilesFromTarget(target, hyDFSfilename, Meta)
			if err != nil {
				continue
			}
			if _, ok := hyDFSFiles[hyDFSfilename]; ok {
				isPresent = true
				fmt.Printf("RingID: %20d | ID: %s\n", GetRingId(id), id)
			}
		}
		if !isPresent {
			fmt.Printf("File %s not found in HyDFS\n", hyDFSfilename)
		} else {
			fmt.Printf("\nFile's RingID: %20d\n", GetRingId(hyDFSfilename))
		}

	case "liststore":
		HyDFSFiles.Range(func(k, v any) bool {
			filename := k.(string)
			fmt.Printf("Filename: %s | RingID: %20d", filename, GetRingId(filename))
			return true
		})

	case "getfromreplica":
		if len(fields) != 4 {
			fmt.Println("Usage: getfromreplica <VMaddress> <HyDFSFilename> <localfilename>")
			return
		}
		targetAddr := fields[1]
		targetIP, targetPortStr, err := net.SplitHostPort(targetAddr)
		if err != nil {
			fmt.Printf("invalid target address: %v\n", err)
			return
		}
		targetPort, err := strconv.Atoi(targetPortStr)
		if err != nil {
			fmt.Printf("invalid target port: %v\n", err)
			return
		}
		hyDFSfilename := fields[2]
		localfilename := fields[3]

		target := Member{
			IP:   targetIP,
			Port: targetPort,
		}

		files, err := getFilesFromTarget(target, hyDFSfilename, One)
		if err != nil {
			fmt.Printf("get files from target error: %v", err)
			return
		}

		hyDFSFile, ok := files[hyDFSfilename]
		if !ok {
			fmt.Printf("file %s not found on replica %s\n", hyDFSfilename, targetAddr)
			return
		}
		var full []byte
		for _, fp := range hyDFSFile.Chunks {
			data, err := DecodeBase64ToBytes(fp.DataB64)
			if err != nil {
				fmt.Printf("file decode error: %v", err)
				return
			}
			full = append(full, data...)
		}

		if err := os.WriteFile(localfilename, full, 0644); err != nil {
			fmt.Printf("failed to write file: %v\n", err)
			return
		}
		fmt.Printf("Retrieved HyDFS file %s from replica %s to %s!\n", hyDFSfilename, targetAddr, localfilename)

	case "multiappend":
		if len(fields) < 4 {
			fmt.Println("Usage: multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilenamei> <localfilenamej> ...")
			return
		}

		hyDFSfilename := fields[1]
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		if len(fields[2:])%2 != 0 {
			fmt.Println("Usage: multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilenamei> <localfilenamej> ...")
			return
		}
		numFiles := (len(fields) - 2) / 2
		targets := fields[2 : 2+numFiles]
		localfilenames := fields[2+numFiles:]

		for i, targetAddr := range targets {
			targetIP, targetPortStr, err := net.SplitHostPort(targetAddr)
			if err != nil {
				fmt.Println("Usage: multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilenamei> <localfilenamej> ...")
				return
			}
			targetPort, err := strconv.Atoi(targetPortStr)
			if err != nil {
				fmt.Println("Usage: multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilenamei> <localfilenamej> ...")
				return
			}
			localfilename := localfilenames[i]

			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", targetIP, targetPort))
			if err != nil {
				fmt.Printf("resolve targetAddr error: %v", err)
				return
			}
			payloadBytes, _ := json.Marshal(MultiAppendRequest{
				HyDFSFilename: hyDFSfilename,
				LocalFilename: localfilename,
			})
			msg := Message{
				MessageType: MultiAppend,
				From:        &self,
				Payload:     payloadBytes,
			}

			sendUDP(targetAddr, &msg)
		}
		fmt.Printf("Multiappend for %s initiated!\n", hyDFSfilename)

	default:
		fmt.Println("Unknown command:", line)
	}
}
