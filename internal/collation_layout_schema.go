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
// DERIVED LAYOUT: collation bounds are stored in the existing lower_bounds and
// upper_bounds maps under synthetic derived field IDs, so data_file gains no
// fields at all.
func collationStatsFields() []avro.SchemaField { return nil }
