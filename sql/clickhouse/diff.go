// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import (
	"fmt"
	"strings"

	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/schema"
)

// DefaultDiff provides basic diffing capabilities for ClickHouse dialects.
// Note, it is recommended to call Open, create a new Driver and use its
// Differ when a database connection is available.
var DefaultDiff schema.Differ = &sqlx.Diff{DiffDriver: &diff{}}

// A diff provides a ClickHouse implementation for sqlx.DiffDriver.
type diff struct{}

// SchemaAttrDiff returns a changeset for migrating schema attributes from one state to the other.
func (*diff) SchemaAttrDiff(_, _ *schema.Schema) []schema.Change {
	return nil
}

// RealmObjectDiff returns a changeset for migrating realm (database) objects
// from one state to the other.
func (*diff) RealmObjectDiff(_, _ *schema.Realm) ([]schema.Change, error) {
	return nil, nil
}

// SchemaObjectDiff returns a changeset for migrating schema objects from
// one state to the other.
func (*diff) SchemaObjectDiff(_, _ *schema.Schema, _ *schema.DiffOptions) ([]schema.Change, error) {
	return nil, nil
}

// TableAttrDiff returns a changeset for migrating table attributes from one state to the other.
func (*diff) TableAttrDiff(_, _ *schema.Table, _ *schema.DiffOptions) ([]schema.Change, error) {
	return nil, nil
}

// ViewAttrChanges returns the changes between the two view attributes.
func (*diff) ViewAttrChanges(from, to *schema.View) []schema.Change {
	var changes []schema.Change
	fromMat, toMat := from.Materialized(), to.Materialized()
	switch {
	case fromMat && !toMat:
		changes = append(changes, &schema.DropAttr{A: &schema.Materialized{}})
	case !fromMat && toMat:
		changes = append(changes, &schema.AddAttr{A: &schema.Materialized{}})
	}
	var fromE, toE Engine
	fromHas := sqlx.Has(from.Attrs, &fromE)
	toHas := sqlx.Has(to.Attrs, &toE)
	if fromHas || toHas {
		fromV := strings.TrimSpace(fromE.V)
		toV := strings.TrimSpace(toE.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &Engine{V: fromV}, To: &Engine{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &Engine{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &Engine{V: toV}})
		}
	}
	var fromO, toO EngineOrderBy
	fromHas = sqlx.Has(from.Attrs, &fromO)
	toHas = sqlx.Has(to.Attrs, &toO)
	if fromHas || toHas {
		fromV := strings.TrimSpace(fromO.V)
		toV := strings.TrimSpace(toO.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &EngineOrderBy{V: fromV}, To: &EngineOrderBy{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &EngineOrderBy{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &EngineOrderBy{V: toV}})
		}
	}
	var fromT, toT EngineTTL
	fromHas = sqlx.Has(from.Attrs, &fromT)
	toHas = sqlx.Has(to.Attrs, &toT)
	if fromHas || toHas {
		fromV := strings.TrimSpace(fromT.V)
		toV := strings.TrimSpace(toT.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &EngineTTL{V: fromV}, To: &EngineTTL{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &EngineTTL{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &EngineTTL{V: toV}})
		}
	}
	var fromS, toS EngineSettings
	fromHas = sqlx.Has(from.Attrs, &fromS)
	toHas = sqlx.Has(to.Attrs, &toS)
	if fromHas || toHas {
		fromV := strings.TrimSpace(fromS.V)
		toV := strings.TrimSpace(toS.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &EngineSettings{V: fromV}, To: &EngineSettings{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &EngineSettings{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &EngineSettings{V: toV}})
		}
	}
	var fromTo, toTo MaterializedViewTo
	fromHas = sqlx.Has(from.Attrs, &fromTo)
	toHas = sqlx.Has(to.Attrs, &toTo)
	if fromHas || toHas {
		fromV := strings.TrimSpace(fromTo.V)
		toV := strings.TrimSpace(toTo.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &MaterializedViewTo{V: fromV}, To: &MaterializedViewTo{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &MaterializedViewTo{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &MaterializedViewTo{V: toV}})
		}
	}
	return changes
}

// ColumnChange returns the schema changes (if any) for migrating one column to the other.
func (*diff) ColumnChange(_ *schema.Table, from, to *schema.Column, _ *schema.DiffOptions) (schema.Change, error) {
	var change schema.ChangeKind
	if from.Type.Null != to.Type.Null {
		change |= schema.ChangeNull
	}
	fromT, err := columnTypeString(from)
	if err != nil {
		return sqlx.NoChange, err
	}
	toT, err := columnTypeString(to)
	if err != nil {
		return sqlx.NoChange, err
	}
	if !strings.EqualFold(fromT, toT) {
		change |= schema.ChangeType
	}
	if change.Is(schema.NoChange) {
		return sqlx.NoChange, nil
	}
	return &schema.ModifyColumn{From: from, To: to, Change: change}, nil
}

// IndexAttrChanged reports if the index attributes were changed.
func (*diff) IndexAttrChanged(from, to []schema.Attr) bool {
	var (
		fromT, toT IndexType
		fromG, toG IndexGranularity
	)
	switch {
	case sqlx.Has(from, &fromT) != sqlx.Has(to, &toT):
		return true
	case strings.TrimSpace(fromT.T) != "" && !strings.EqualFold(fromT.T, toT.T):
		return true
	case sqlx.Has(from, &fromG) != sqlx.Has(to, &toG):
		return true
	case fromG.V != toG.V:
		return true
	default:
		return false
	}
}

// IndexPartAttrChanged reports if the part's attributes at position "i" were changed.
func (*diff) IndexPartAttrChanged(_, _ *schema.Index, _ int) bool {
	return false
}

// IsGeneratedIndexName reports if the index name was generated by the database.
func (*diff) IsGeneratedIndexName(*schema.Table, *schema.Index) bool {
	return false
}

// ReferenceChanged reports if the foreign key referential action was changed.
func (*diff) ReferenceChanged(_, _ schema.ReferenceOption) bool {
	return false
}

// ForeignKeyAttrChanged reports if any of the foreign-key attributes were changed.
func (*diff) ForeignKeyAttrChanged(_, _ []schema.Attr) bool {
	return false
}

func columnTypeString(c *schema.Column) (string, error) {
	if c.Type == nil {
		return "", fmt.Errorf("clickhouse: missing type for column %q", c.Name)
	}
	base := c.Type.Raw
	if base == "" {
		ft, err := FormatType(c.Type.Type)
		if err != nil {
			return "", err
		}
		base = ft
	}
	return wrapNullable(base, c.Type.Null), nil
}
