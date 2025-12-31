// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import "ariga.io/atlas/sql/schema"

// Engine attribute describes the storage engine used to create a table or view.
type Engine struct {
	schema.Attr
	V string
}

// EngineOrderBy describes the ORDER BY clause for the table engine.
type EngineOrderBy struct {
	schema.Attr
	V string
}

// EngineTTL describes the TTL clause for the table engine.
type EngineTTL struct {
	schema.Attr
	V string
}

// EngineSettings describes the SETTINGS clause for the table engine.
type EngineSettings struct {
	schema.Attr
	V string
}

// EnginePartitionBy describes the PARTITION BY clause for the table engine.
type EnginePartitionBy struct {
	schema.Attr
	V string
}

// MaterializedViewTo describes the target table for a materialized view.
type MaterializedViewTo struct {
	schema.Attr
	V string
}

// ColumnCodec describes the CODEC clause for a column.
type ColumnCodec struct {
	schema.Attr
	V string
}

// IndexType attribute describes the data skipping index type.
type IndexType struct {
	schema.Attr
	T string
}

// IndexGranularity attribute describes the data skipping index granularity.
type IndexGranularity struct {
	schema.Attr
	V int
}
