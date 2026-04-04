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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCompactionTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "compaction_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func TestAnalyzeCompaction_SmallFiles(t *testing.T) {
	tbl := newCompactionTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(
		tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 5 small data files (one per commit → 5 separate manifests → 5 FileScanTasks)
	for i := range 5 {
		dataPath := fmt.Sprintf("%s/data/file-%d.parquet",
			tbl.Location(), i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "row-%d"}]`, i+1, i+1))

		tx := tbl.NewTransaction()
		err := tx.AddFiles(t.Context(), []string{dataPath}, nil, false)
		require.NoError(t, err)

		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	// Verify: 5 rows scannable
	assertRowCount(t, tbl, 5)

	// Analyze with low thresholds to trigger compaction
	cfg := table.CompactionConfig{
		TargetFileSizeBytes: 10 * 1024 * 1024, // 10MB
		MinFileSizeBytes:    5 * 1024 * 1024,  // 5MB
		MaxFileSizeBytes:    20 * 1024 * 1024, // 20MB
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	plan, err := table.AnalyzeCompaction(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 5, plan.TotalInputFiles)
	assert.Greater(t, plan.TotalInputBytes, int64(0))

	// All 5 files are tiny (< 5MB min) → all are candidates
	require.NotEmpty(t, plan.Groups, "expected compaction groups for small files")
	assert.Equal(t, 0, plan.SkippedFiles)

	totalInGroups := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
		assert.Equal(t, "", g.PartitionKey, "unpartitioned table should have empty key")
	}
	assert.Equal(t, 5, totalInGroups)
	assert.Greater(t, plan.EstOutputFiles, 0)
}

func TestAnalyzeCompaction_AllOptimal(t *testing.T) {
	tbl := newCompactionTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(
		tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := fmt.Sprintf("%s/data/file-0.parquet", tbl.Location())
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	tx := tbl.NewTransaction()
	err = tx.AddFiles(t.Context(), []string{dataPath}, nil, false)
	require.NoError(t, err)

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Single file, MinInputFiles=5 → no groups
	cfg := table.CompactionConfig{
		TargetFileSizeBytes: 100,
		MinFileSizeBytes:    1,
		MaxFileSizeBytes:    1024 * 1024 * 1024,
		MinInputFiles:       5,
		DeleteFileThreshold: 5,
	}

	plan, err := table.AnalyzeCompaction(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 1, plan.TotalInputFiles)
	assert.Empty(t, plan.Groups, "single file below MinInputFiles should produce no groups")
}

func TestAnalyzeCompaction_WithPositionDeletes(t *testing.T) {
	tbl := newCompactionTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(
		tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 data files
	for i := range 3 {
		dataPath := fmt.Sprintf("%s/data/file-%d.parquet", tbl.Location(), i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "row-%d"}, {"id": %d, "data": "row-%d"}]`,
			i*2+1, i*2+1, i*2+2, i*2+2))

		tx := tbl.NewTransaction()
		err := tx.AddFiles(t.Context(), []string{dataPath}, nil, false)
		require.NoError(t, err)

		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	// Add a position delete targeting the first data file
	firstDataPath := fmt.Sprintf("%s/data/file-0.parquet", tbl.Location())
	posDelPath := fmt.Sprintf("%s/data/pos-del-001.parquet", tbl.Location())
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 0}]`, firstDataPath))

	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(posDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Verify: 5 rows after delete (6 original - 1 deleted)
	assertRowCount(t, tbl, 5)

	cfg := table.CompactionConfig{
		TargetFileSizeBytes: 10 * 1024 * 1024,
		MinFileSizeBytes:    5 * 1024 * 1024,
		MaxFileSizeBytes:    20 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 1,
	}

	plan, err := table.AnalyzeCompaction(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 3, plan.TotalInputFiles)
	require.NotEmpty(t, plan.Groups)

	totalDeletes := 0
	for _, g := range plan.Groups {
		totalDeletes += g.DeleteFileCount
	}
	assert.Greater(t, totalDeletes, 0, "should detect delete files in compaction plan")
}

func TestAnalyzeCompaction_EmptyTable(t *testing.T) {
	tbl := newCompactionTestTable(t)

	cfg := table.CompactionConfig{
		TargetFileSizeBytes: 10 * 1024 * 1024,
		MinFileSizeBytes:    5 * 1024 * 1024,
		MaxFileSizeBytes:    20 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	plan, err := table.AnalyzeCompaction(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 0, plan.TotalInputFiles)
	assert.Empty(t, plan.Groups)
}

func TestAnalyzeCompaction_InvalidConfig(t *testing.T) {
	tbl := newCompactionTestTable(t)

	cfg := table.CompactionConfig{TargetFileSizeBytes: 0}
	_, err := table.AnalyzeCompaction(t.Context(), tbl, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid compaction config")
}
