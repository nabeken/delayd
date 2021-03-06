package delayd

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

// NewUUID returns uuid
func NewUUID() ([]byte, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) {
		return nil, errors.New("util: could not create uuid")
	}
	if err != nil {
		return nil, err
	}

	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return uuid, nil
}

// Converts bytes to an integer
func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// Converts a uint to a byte slice
func uint32ToBytes(u uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}
