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

package iceberg

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// This file holds the ACTIVE collation bounds layout. Together with
// internal/collation_layout_schema.go it is the only place where the on-disk
// shape of collation-aware bounds is defined; swapping both files swaps the
// layout without touching the rest of the code.
//
// DERIVED LAYOUT: each (column, collation, version) is a derived,
// non-materialized field COLLATE(column, collation, version) with its own field
// ID. Its bounds go in the existing lower_bounds/upper_bounds maps under that
// ID, so a data file stores one extra entry per version in each map and no
// version string; the (column, collation, version) identity lives once in the
// table schema as the derived-field definition.
//
// Prototype simplifications:
//   - The derived-field registry lives in process memory instead of the table
//     schema. tableMetadata reports the size of the definitions a real
//     implementation would add to the schema.
//   - Code-point ranges are not stored; the derived-field proposal does not
//     define them.

// derivedFieldIDBase is the first synthetic field ID assigned to a derived
// collation field.
const derivedFieldIDBase = 10000

var collationLayout collationBoundsLayout = &derivedLayout{
	ids:  make(map[CollationVersionRef]int),
	refs: make(map[int]CollationVersionRef),
}

// collationStatsValue is unused: this layout adds no data_file field, and the
// struct field mapped to it is absent from the Avro schema.
type collationStatsValue struct{}

type derivedLayout struct {
	mu   sync.Mutex
	ids  map[CollationVersionRef]int
	refs map[int]CollationVersionRef
}

func (*derivedLayout) name() string { return "derived COLLATE() fields" }

// fieldID returns the derived field ID for ref, assigning the next one if ref
// has none yet.
func (l *derivedLayout) fieldID(ref CollationVersionRef) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if id, ok := l.ids[ref]; ok {
		return id
	}
	id := derivedFieldIDBase + len(l.ids)
	l.ids[ref] = id
	l.refs[id] = ref

	return id
}

func (l *derivedLayout) lookup(id int) (CollationVersionRef, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ref, ok := l.refs[id]

	return ref, ok
}

func (l *derivedLayout) encode(d *dataFile, stats map[int]CollationColumnStats) {
	if len(stats) == 0 {
		return
	}

	var lower, upper []colMap[int, []byte]
	if d.LowerBounds != nil {
		lower = *d.LowerBounds
	}
	if d.UpperBounds != nil {
		upper = *d.UpperBounds
	}

	// sorted so derived field IDs are assigned deterministically
	for _, id := range slices.Sorted(maps.Keys(stats)) {
		for _, e := range stats[id].Bounds {
			fid := l.fieldID(CollationVersionRef{FieldID: id, Collation: e.Collation, Version: e.Version})
			lower = append(lower, colMap[int, []byte]{Key: fid, Value: e.Lower})
			upper = append(upper, colMap[int, []byte]{Key: fid, Value: e.Upper})
		}
	}
	d.LowerBounds, d.UpperBounds = &lower, &upper
}

func (l *derivedLayout) decode(d *dataFile) map[int]CollationColumnStats {
	var out map[int]CollationColumnStats
	for fid, lo := range d.lowerBoundMap {
		ref, ok := l.lookup(fid)
		if !ok {
			continue
		}
		hi, ok := d.upperBoundMap[fid]
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[int]CollationColumnStats)
		}
		s := out[ref.FieldID]
		s.Bounds = append(s.Bounds, CollationBoundEntry{
			Collation: ref.Collation, Version: ref.Version, Lower: lo, Upper: hi,
		})
		out[ref.FieldID] = s
	}
	for id, s := range out {
		slices.SortFunc(s.Bounds, func(a, b CollationBoundEntry) int { return cmp.Compare(a.Version, b.Version) })
		out[id] = s
	}

	return out
}

// derivedFieldJSON is the definition a derived collation field would add to the
// table schema.
type derivedFieldJSON struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Required  bool   `json:"required"`
	SourceID  int    `json:"source-id"`
	Transform string `json:"transform"`
}

func (l *derivedLayout) tableMetadata(refs []CollationVersionRef) []byte {
	if len(refs) == 0 {
		return nil
	}

	defs := make([]derivedFieldJSON, 0, len(refs))
	for _, ref := range refs {
		fid := l.fieldID(ref)
		defs = append(defs, derivedFieldJSON{
			ID:        fid,
			Name:      fmt.Sprintf("_collate_%d", fid),
			Type:      "string",
			SourceID:  ref.FieldID,
			Transform: fmt.Sprintf("collate[%s, %s]", ref.Collation, ref.Version),
		})
	}
	out, err := json.Marshal(defs)
	if err != nil {
		return nil
	}

	return out
}
