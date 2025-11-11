package main

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"

	"github.com/google/uuid"
)

func GetUUID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func KeyFor(m Member) string { return fmt.Sprintf("%s:%d:%s", m.IP, m.Port, m.Timestamp) }

func GetRingId(s string) uint64 {
	sum := sha1.Sum([]byte(s))              // 160-bit hash (20 bytes)
	return binary.BigEndian.Uint64(sum[:8]) // take the first 8 bytes as a 64-bit number
}

func GetRingSuccessor(ringId uint64, membershipList map[string]Member) Member {
	var successor Member
	minDiff := ^uint64(0) // max uint64

	for _, m := range membershipList {
		if m.Status != Alive || m.RingID == ringId {
			continue
		}

		diff := m.RingID - ringId // unsigned wraparound handles ring
		if diff < minDiff {
			minDiff = diff
			successor = m
		}
	}

	return successor
}

func GetRingPredecessor(ringId uint64, membershipList map[string]Member) Member {
	var predecessor Member
	minDiff := ^uint64(0) // max uint64

	for _, m := range membershipList {
		if m.Status != Alive || m.RingID == ringId {
			continue
		}

		// distance going *backwards* around the ring
		diff := ringId - m.RingID // wraps automatically for uint64
		if diff < minDiff {
			minDiff = diff
			predecessor = m
		}
	}

	return predecessor
}

func SnapshotMembers(omitFailed bool) map[string]Member {
	out := make(map[string]Member)
	MembershipList.Range(func(k, v any) bool {
		if omitFailed && v.(Member).Status == Failed {
			return true
		}
		out[k.(string)] = v.(Member)
		return true
	})
	return out
}

func SelectKMembers(members map[string]Member, k int) []Member {
	slice := make([]Member, 0, len(members))
	for _, m := range members {
		slice = append(slice, m)
	}

	// Fewer than k members
	if len(slice) <= k {
		return slice
	}

	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})

	return slice[:k]
}

func EncodeFileToBase64(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func DecodeBase64ToBytes(dataB64 string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetHyDFSCompliantFilename(filename string) string {
	return strings.ReplaceAll(filename, "/", "_")
}
