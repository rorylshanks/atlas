// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import (
	"fmt"
	"strings"
	"unicode"

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
func (*diff) TableAttrDiff(from, to *schema.Table, _ *schema.DiffOptions) ([]schema.Change, error) {
	return tableAttrChanges(from, to), nil
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
		fromV := normalizeSpace(fromE.V)
		toV := normalizeSpace(toE.V)
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
		fromV := normalizeSpace(fromO.V)
		toV := normalizeSpace(toO.V)
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
		fromV := normalizeSpace(fromT.V)
		toV := normalizeSpace(toT.V)
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
		fromV := normalizeSpace(fromS.V)
		toV := normalizeSpace(toS.V)
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
		fromV := normalizeSpace(fromTo.V)
		toV := normalizeSpace(toTo.V)
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

func tableAttrChanges(from, to *schema.Table) []schema.Change {
	var changes []schema.Change
	var fromE, toE Engine
	fromHas := sqlx.Has(from.Attrs, &fromE)
	toHas := sqlx.Has(to.Attrs, &toE)
	if fromHas || toHas {
		fromV := normalizeSpace(fromE.V)
		toV := normalizeSpace(toE.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &Engine{V: fromV}, To: &Engine{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &Engine{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &Engine{V: toV}})
		}
	}
	var fromP, toP EnginePartitionBy
	fromHas = sqlx.Has(from.Attrs, &fromP)
	toHas = sqlx.Has(to.Attrs, &toP)
	if fromHas || toHas {
		fromV := normalizeSpace(fromP.V)
		toV := normalizeSpace(toP.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &EnginePartitionBy{V: fromV}, To: &EnginePartitionBy{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &EnginePartitionBy{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &EnginePartitionBy{V: toV}})
		}
	}
	var fromO, toO EngineOrderBy
	fromHas = sqlx.Has(from.Attrs, &fromO)
	toHas = sqlx.Has(to.Attrs, &toO)
	if fromHas || toHas {
		fromV := normalizeSpace(fromO.V)
		toV := normalizeSpace(toO.V)
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
		fromV := normalizeSpace(fromT.V)
		toV := normalizeSpace(toT.V)
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
		fromV := normalizeSpace(fromS.V)
		toV := normalizeSpace(toS.V)
		switch {
		case fromHas && toHas && !strings.EqualFold(fromV, toV):
			changes = append(changes, &schema.ModifyAttr{From: &EngineSettings{V: fromV}, To: &EngineSettings{V: toV}})
		case fromHas && !toHas:
			changes = append(changes, &schema.DropAttr{A: &EngineSettings{V: fromV}})
		case !fromHas && toHas:
			changes = append(changes, &schema.AddAttr{A: &EngineSettings{V: toV}})
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
	if !strings.EqualFold(normalizeSpace(fromT), normalizeSpace(toT)) {
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
	case normalizeSpace(fromT.T) != "" && !strings.EqualFold(normalizeSpace(fromT.T), normalizeSpace(toT.T)):
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
	return normalizeSpace(wrapNullable(base, c.Type.Null)), nil
}

func normalizeSpace(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var (
		b       strings.Builder
		inQuote rune
		escape  bool
	)
	b.Grow(len(s))
	for _, r := range s {
		if inQuote != 0 {
			b.WriteRune(r)
			if escape {
				escape = false
				continue
			}
			if r == '\\' {
				escape = true
				continue
			}
			if r == inQuote {
				inQuote = 0
			}
			continue
		}
		switch r {
		case '\'', '"':
			inQuote = r
			b.WriteRune(r)
		default:
			if unicode.IsSpace(r) {
				continue
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ViewDefChanged reports if view definitions differ ignoring whitespace-only changes.
func (*diff) ViewDefChanged(v1, v2 *schema.View) bool {
	return normalizeSpace(sqlx.TrimViewExtra(v1.Def)) != normalizeSpace(sqlx.TrimViewExtra(v2.Def))
}
