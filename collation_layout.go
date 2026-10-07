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

// This file holds the ACTIVE collation bounds layout. Together with
// internal/collation_layout_schema.go it is the only place where the on-disk
// shape of collation-aware bounds is defined; swapping both files swaps the
// layout without touching the rest of the code.
//
// INLINE LAYOUT (spec PR apache/iceberg#16972): the collated column's own stats
// carry min/max code point and a list of {version, lower_bound, upper_bound},
// one element per collation version. The version string is stored in every data
// file; the collation name is not stored per entry (the field's declared
// collation applies). See internal/collation_layout_schema.go for how the v4
// content_stats struct is emulated in a v3 manifest.

var collationLayout collationBoundsLayout = inlineLayout{}

// collationStatsValue is the Avro value of the data_file.collation_stats map.
type collationStatsValue struct {
	MinCodePoint *int32        `avro:"min_code_point"`
	MaxCodePoint *int32        `avro:"max_code_point"`
	Bounds       []inlineBound `avro:"collation_bounds"`
}

type inlineBound struct {
	Version string `avro:"version"`
	Lower   []byte `avro:"lower_bound"`
	Upper   []byte `avro:"upper_bound"`
}

type inlineLayout struct{}

func (inlineLayout) name() string { return "inline content_stats" }

func (inlineLayout) encode(d *dataFile, stats map[int]CollationColumnStats) {
	if len(stats) == 0 {
		return
	}

	values := make(map[int]collationStatsValue, len(stats))
	for id, s := range stats {
		bounds := make([]inlineBound, len(s.Bounds))
		for i, e := range s.Bounds {
			bounds[i] = inlineBound{Version: e.Version, Lower: e.Lower, Upper: e.Upper}
		}
		values[id] = collationStatsValue{MinCodePoint: s.MinCodePoint, MaxCodePoint: s.MaxCodePoint, Bounds: bounds}
	}
	d.CollationStatsData = mapToAvroColMap(values)
}

func (inlineLayout) decode(d *dataFile) map[int]CollationColumnStats {
	if d.CollationStatsData == nil {
		return nil
	}

	out := make(map[int]CollationColumnStats, len(*d.CollationStatsData))
	for _, kv := range *d.CollationStatsData {
		bounds := make([]CollationBoundEntry, len(kv.Value.Bounds))
		for i, b := range kv.Value.Bounds {
			bounds[i] = CollationBoundEntry{Version: b.Version, Lower: b.Lower, Upper: b.Upper}
		}
		out[kv.Key] = CollationColumnStats{
			MinCodePoint: kv.Value.MinCodePoint,
			MaxCodePoint: kv.Value.MaxCodePoint,
			Bounds:       bounds,
		}
	}

	return out
}

func (inlineLayout) tableMetadata([]CollationVersionRef) []byte { return nil }
