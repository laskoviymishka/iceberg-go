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

package internal

import "github.com/twmb/avro"

// This file holds the Avro schema fragment for the ACTIVE collation bounds
// layout. It is swapped together with collation_layout.go in the iceberg
// package: the two files are the only place where the on-disk shape of
// collation-aware bounds is defined.
//
// INLINE LAYOUT (spec PR apache/iceberg#16972): the collated column's own stats
// carry
//
//	min_code_point: int, max_code_point: int,
//	collation_bounds: list<{version, lower_bound, upper_bound}>  // one per version
//
// The spec places these in the v4 content_stats struct (offsets 8, 9, 18-22).
// This v3 manifest emulates that struct as a data_file.collation_stats map keyed
// by field ID; field IDs 9000+offset mirror the content_stats offsets. A v4
// content_stats struct is positional and does not store the map key, so these
// numbers are slightly pessimistic for the inline layout.
func collationStatsFields() []avro.SchemaField {
	bound := avro.SchemaNode{
		Type: "record",
		Name: "collation_bound",
		Fields: []avro.SchemaField{
			fieldNode("version", StringNode, 9020),
			fieldNode("lower_bound", BytesNode, 9021),
			fieldNode("upper_bound", BytesNode, 9022),
		},
	}
	value := avro.SchemaNode{
		Type: "record",
		Name: "collation_column_stats",
		Fields: []avro.SchemaField{
			fieldNode("min_code_point", NullableNode(IntNode), 9008, withDefault(nil)),
			fieldNode("max_code_point", NullableNode(IntNode), 9009, withDefault(nil)),
			fieldNode("collation_bounds",
				avro.SchemaNode{Type: "array", Items: &bound, Props: map[string]any{"element-id": 9019}},
				9018),
		},
	}

	return []avro.SchemaField{
		fieldNode("collation_stats",
			NullableNode(newMapNode("k9001_v9002", IntNode, value, 9001, 9002)),
			9000, withDefault(nil),
			withDoc("map of column id to inline collation stats (emulates v4 content_stats)")),
	}
}
