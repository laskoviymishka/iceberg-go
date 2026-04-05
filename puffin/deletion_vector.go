// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package puffin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// dvMagic is the magic bytes identifying a deletion-vector-v1 blob.
var dvMagic = [4]byte{0xD1, 0xD3, 0x39, 0x64}

// dvMinSize is the minimum size of a valid DV blob:
// 4 (length) + 4 (magic) + 4 (CRC) = 12 bytes (with empty bitmap)
const dvMinSize = 12

// DeserializeDeletionVector parses a deletion-vector-v1 blob from a puffin file
// and returns the sorted set of deleted row positions.
//
// The blob format is:
//
//	[4 bytes BE uint32: byte count of magic+vector+crc]
//	[4 bytes magic: 0xD1 0xD3 0x39 0x64]
//	[N bytes: Roaring64 portable format bitmap]
//	[4 bytes BE uint32: CRC-32 over magic+vector]
func DeserializeDeletionVector(data []byte) ([]int64, error) {
	if len(data) < dvMinSize {
		return nil, fmt.Errorf("deletion vector blob too short: %d bytes, minimum %d", len(data), dvMinSize)
	}

	// Read outer length (big-endian uint32): byte count of magic + vector + CRC.
	outerLen := binary.BigEndian.Uint32(data[0:4])
	if int(outerLen) != len(data)-4 {
		return nil, fmt.Errorf("deletion vector length mismatch: header says %d, have %d",
			outerLen, len(data)-4)
	}

	// Validate magic bytes.
	if !bytes.Equal(data[4:8], dvMagic[:]) {
		return nil, fmt.Errorf("invalid deletion vector magic: got %x, want %x", data[4:8], dvMagic)
	}

	// Extract vector bytes and stored CRC.
	vectorBytes := data[8 : len(data)-4]
	storedCRC := binary.BigEndian.Uint32(data[len(data)-4:])

	// Verify CRC-32 (IEEE) over magic + vector.
	computed := crc32.ChecksumIEEE(data[4 : len(data)-4])
	if computed != storedCRC {
		return nil, fmt.Errorf("deletion vector CRC mismatch: stored %08x, computed %08x",
			storedCRC, computed)
	}

	// Deserialize the Roaring64 portable format bitmap.
	bm := roaring64.New()
	if _, err := bm.ReadFrom(bytes.NewReader(vectorBytes)); err != nil {
		return nil, fmt.Errorf("deserialize roaring64 bitmap: %w", err)
	}

	// Convert to sorted []int64.
	positions := bm.ToArray()
	result := make([]int64, len(positions))
	for i, v := range positions {
		result[i] = int64(v)
	}

	return result, nil
}

// SerializeDeletionVector encodes a set of deleted row positions as a
// deletion-vector-v1 blob suitable for writing into a puffin file.
//
// Positions must be non-negative. The output follows the format described
// in DeserializeDeletionVector.
func SerializeDeletionVector(positions []int64) ([]byte, error) {
	bm := roaring64.New()
	for _, p := range positions {
		if p < 0 {
			return nil, fmt.Errorf("deletion vector position must be non-negative, got %d", p)
		}
		bm.Add(uint64(p))
	}

	// Serialize the Roaring64 bitmap.
	var vectorBuf bytes.Buffer
	if _, err := bm.WriteTo(&vectorBuf); err != nil {
		return nil, fmt.Errorf("serialize roaring64 bitmap: %w", err)
	}
	vectorBytes := vectorBuf.Bytes()

	// Build the blob: length(4) + magic(4) + vector(N) + CRC(4)
	blobSize := 4 + 4 + len(vectorBytes) + 4
	buf := make([]byte, blobSize)

	// Outer length: magic(4) + vector(N) + CRC(4)
	binary.BigEndian.PutUint32(buf[0:4], uint32(4+len(vectorBytes)+4))

	// Magic.
	copy(buf[4:8], dvMagic[:])

	// Vector.
	copy(buf[8:8+len(vectorBytes)], vectorBytes)

	// CRC-32 over magic + vector.
	crc := crc32.ChecksumIEEE(buf[4 : 8+len(vectorBytes)])
	binary.BigEndian.PutUint32(buf[8+len(vectorBytes):], crc)

	return buf, nil
}

// DeletionVectorCardinality returns the number of deleted positions in
// a deletion-vector-v1 blob without fully deserializing it.
func DeletionVectorCardinality(data []byte) (uint64, error) {
	if len(data) < dvMinSize {
		return 0, fmt.Errorf("deletion vector blob too short: %d bytes", len(data))
	}

	if !bytes.Equal(data[4:8], dvMagic[:]) {
		return 0, fmt.Errorf("invalid deletion vector magic: got %x", data[4:8])
	}

	vectorBytes := data[8 : len(data)-4]
	bm := roaring64.New()
	if _, err := bm.ReadFrom(bytes.NewReader(vectorBytes)); err != nil {
		return 0, fmt.Errorf("deserialize roaring64 bitmap: %w", err)
	}

	return bm.GetCardinality(), nil
}
