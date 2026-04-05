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

package puffin_test

import (
	"encoding/binary"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeletionVector_RoundTrip(t *testing.T) {
	positions := []int64{0, 5, 10, 42, 100, 999}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	result, err := puffin.DeserializeDeletionVector(data)
	require.NoError(t, err)

	assert.Equal(t, positions, result)
}

func TestDeletionVector_RoundTripEmpty(t *testing.T) {
	data, err := puffin.SerializeDeletionVector(nil)
	require.NoError(t, err)

	result, err := puffin.DeserializeDeletionVector(data)
	require.NoError(t, err)

	assert.Empty(t, result)
}

func TestDeletionVector_RoundTripSinglePosition(t *testing.T) {
	positions := []int64{42}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	result, err := puffin.DeserializeDeletionVector(data)
	require.NoError(t, err)

	assert.Equal(t, positions, result)
}

func TestDeletionVector_RoundTripLargePositions(t *testing.T) {
	// Positions spanning multiple Roaring64 high-word buckets
	positions := []int64{0, 1, 1<<32 - 1, 1 << 32, 1<<32 + 1, 1 << 40, 1<<50 + 42}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	result, err := puffin.DeserializeDeletionVector(data)
	require.NoError(t, err)

	assert.Equal(t, positions, result)
}

func TestDeletionVector_RoundTripUnsorted(t *testing.T) {
	// Input is unsorted — output should be sorted
	positions := []int64{100, 5, 42, 0, 999}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	result, err := puffin.DeserializeDeletionVector(data)
	require.NoError(t, err)

	assert.Equal(t, []int64{0, 5, 42, 100, 999}, result)
}

func TestDeletionVector_RoundTripDuplicates(t *testing.T) {
	// Duplicates should be deduplicated
	positions := []int64{5, 5, 10, 10, 10}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	result, err := puffin.DeserializeDeletionVector(data)
	require.NoError(t, err)

	assert.Equal(t, []int64{5, 10}, result)
}

func TestDeletionVector_Cardinality(t *testing.T) {
	positions := []int64{1, 2, 3, 100, 200}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	card, err := puffin.DeletionVectorCardinality(data)
	require.NoError(t, err)

	assert.Equal(t, uint64(5), card)
}

func TestDeletionVector_DeserializeTooShort(t *testing.T) {
	_, err := puffin.DeserializeDeletionVector([]byte{0x01, 0x02})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestDeletionVector_DeserializeBadMagic(t *testing.T) {
	// Valid length header but wrong magic
	data := make([]byte, 12)
	binary.BigEndian.PutUint32(data[0:4], 8) // length = magic(4) + crc(4)
	data[4] = 0xFF                           // bad magic

	_, err := puffin.DeserializeDeletionVector(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "magic")
}

func TestDeletionVector_DeserializeBadCRC(t *testing.T) {
	positions := []int64{1, 2, 3}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	// Corrupt the CRC (last 4 bytes)
	data[len(data)-1] ^= 0xFF

	_, err = puffin.DeserializeDeletionVector(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CRC")
}

func TestDeletionVector_DeserializeLengthMismatch(t *testing.T) {
	positions := []int64{1}

	data, err := puffin.SerializeDeletionVector(positions)
	require.NoError(t, err)

	// Truncate the data but keep the length header intact
	truncated := data[:len(data)-2]

	_, err = puffin.DeserializeDeletionVector(truncated)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "length mismatch")
}

func TestDeletionVector_SerializeNegativePosition(t *testing.T) {
	_, err := puffin.SerializeDeletionVector([]int64{-1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-negative")
}
