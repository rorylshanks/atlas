// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/schema"
)

// A diff provides a ClickHouse implementation for schema.Inspector.
type inspect struct{ *conn }

var _ schema.Inspector = (*inspect)(nil)

// InspectRealm returns schema descriptions of all resources in the given realm.
func (i *inspect) InspectRealm(ctx context.Context, opts *schema.InspectRealmOption) (*schema.Realm, error) {
	schemas, err := i.schemas(ctx, opts)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &schema.InspectRealmOption{}
	}
	var (
		mode = sqlx.ModeInspectRealm(opts)
		r    = schema.NewRealm(schemas...)
	)
	if len(schemas) > 0 && (mode.Is(schema.InspectTables) || mode.Is(schema.InspectViews)) {
		if err := i.tables(ctx, r, nil); err != nil {
			return nil, err
		}
		if err := i.columns(ctx, r, nil); err != nil {
			return nil, err
		}
		if err := i.indexes(ctx, r, nil); err != nil {
			return nil, err
		}
	}
	return schema.ExcludeRealm(r, opts.Exclude)
}

// InspectSchema returns schema descriptions of the tables in the given schema.
// If the schema name is empty, the result will be the attached schema.
func (i *inspect) InspectSchema(ctx context.Context, name string, opts *schema.InspectOptions) (*schema.Schema, error) {
	schemas, err := i.schemas(ctx, &schema.InspectRealmOption{Schemas: []string{name}})
	if err != nil {
		return nil, err
	}
	switch n := len(schemas); {
	case n == 0:
		return nil, &schema.NotExistError{Err: fmt.Errorf("clickhouse: schema %q was not found", name)}
	case n > 1:
		return nil, fmt.Errorf("clickhouse: %d schemas were found for %q", n, name)
	}
	if opts == nil {
		opts = &schema.InspectOptions{}
	}
	var (
		mode = sqlx.ModeInspectSchema(opts)
		r    = schema.NewRealm(schemas...)
	)
	if mode.Is(schema.InspectTables) || mode.Is(schema.InspectViews) {
		if err := i.tables(ctx, r, opts); err != nil {
			return nil, err
		}
		if err := i.columns(ctx, r, opts); err != nil {
			return nil, err
		}
		if err := i.indexes(ctx, r, opts); err != nil {
			return nil, err
		}
	}
	return schema.ExcludeSchema(r.Schemas[0], opts.Exclude)
}

// schemas returns the list of the schemas in the database.
func (i *inspect) schemas(ctx context.Context, opts *schema.InspectRealmOption) ([]*schema.Schema, error) {
	var (
		args  []any
		query = schemasQuery
	)
	var filterSystem bool
	if opts != nil {
		switch n := len(opts.Schemas); {
		case n == 1 && opts.Schemas[0] == "":
			query = currentSchemaQuery
		case n == 1 && opts.Schemas[0] != "":
			query = fmt.Sprintf(schemasQueryArgs, "= ?")
			args = append(args, opts.Schemas[0])
		case n > 0:
			query = fmt.Sprintf(schemasQueryArgs, "IN ("+nArgs(len(opts.Schemas))+")")
			for _, s := range opts.Schemas {
				args = append(args, s)
			}
		}
		filterSystem = len(opts.Schemas) == 0
	} else {
		filterSystem = true
	}
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: querying schemas: %w", err)
	}
	defer rows.Close()
	var schemas []*schema.Schema
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if filterSystem && isSystemSchema(name) {
			continue
		}
		schemas = append(schemas, &schema.Schema{Name: name})
	}
	return schemas, nil
}

func isSystemSchema(name string) bool {
	switch {
	case strings.EqualFold(name, "system"):
		return true
	case strings.EqualFold(name, "information_schema"):
		return true
	case strings.EqualFold(name, "INFORMATION_SCHEMA"):
		return true
	default:
		return false
	}
}

func (i *inspect) tables(ctx context.Context, realm *schema.Realm, opts *schema.InspectOptions) error {
	var (
		args  []any
		query = fmt.Sprintf(tablesQuery, nArgs(len(realm.Schemas)))
	)
	for _, s := range realm.Schemas {
		args = append(args, s.Name)
	}
	if opts != nil && len(opts.Tables) > 0 {
		for _, t := range opts.Tables {
			args = append(args, t)
		}
		query = fmt.Sprintf(tablesQueryArgs, nArgs(len(realm.Schemas)), nArgs(len(opts.Tables)))
	}
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("clickhouse: querying tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name, engine, engineFull, create sql.NullString
			db                               string
		)
		if err := rows.Scan(&db, &name, &engine, &engineFull, &create); err != nil {
			return fmt.Errorf("clickhouse: scanning table: %w", err)
		}
		if name.String == "" {
			continue
		}
		s, ok := realm.Schema(db)
		if !ok {
			return fmt.Errorf("clickhouse: schema %q was not found in realm", db)
		}
		engineName := strings.TrimSpace(engine.String)
		switch engineName {
		case "View", "MaterializedView":
			var (
				def string
				to  string
			)
			if engineName == "MaterializedView" {
				to, def = parseMaterializedView(create.String)
			} else {
				def = parseViewDef(create.String)
			}
			v := schema.NewView(name.String, def)
			if engineName == "MaterializedView" {
				v.SetMaterialized(true)
				if strings.TrimSpace(to) != "" {
					v.AddAttrs(&MaterializedViewTo{V: strings.TrimSpace(to)})
				}
			}
			s.AddViews(v)
		default:
			t := schema.NewTable(name.String)
			s.AddTables(t)
			engineValue := strings.TrimSpace(engineFull.String)
			if engineValue == "" {
				engineValue = strings.TrimSpace(engine.String)
			}
			if engineValue != "" {
				eng, partitionBy, orderBy, ttl, settings := splitEngineFull(engineValue)
				if eng != "" {
					t.AddAttrs(&Engine{V: eng})
				}
				if partitionBy != "" {
					t.AddAttrs(&EnginePartitionBy{V: partitionBy})
				}
				if orderBy != "" {
					t.AddAttrs(&EngineOrderBy{V: orderBy})
				}
				if ttl != "" {
					t.AddAttrs(&EngineTTL{V: ttl})
				}
				if settings != "" {
					t.AddAttrs(&EngineSettings{V: settings})
				}
			}
		}
	}
	return nil
}

func (i *inspect) columns(ctx context.Context, realm *schema.Realm, opts *schema.InspectOptions) error {
	var (
		args  []any
		query = fmt.Sprintf(columnsQuery, nArgs(len(realm.Schemas)))
	)
	for _, s := range realm.Schemas {
		args = append(args, s.Name)
	}
	if opts != nil && len(opts.Tables) > 0 {
		for _, t := range opts.Tables {
			args = append(args, t)
		}
		query = fmt.Sprintf(columnsQueryArgs, nArgs(len(realm.Schemas)), nArgs(len(opts.Tables)))
	}
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil && strings.Contains(err.Error(), "is_nullable") {
		query = fmt.Sprintf(columnsQueryNoNullable, nArgs(len(realm.Schemas)))
		if opts != nil && len(opts.Tables) > 0 {
			query = fmt.Sprintf(columnsQueryArgsNoNullable, nArgs(len(realm.Schemas)), nArgs(len(opts.Tables)))
		}
		rows, err = i.QueryContext(ctx, query, args...)
	}
	if err != nil && missingColumn(err, "compression_codec") {
		if strings.Contains(query, "is_nullable") {
			query = fmt.Sprintf(columnsQueryNoCodec, nArgs(len(realm.Schemas)))
			if opts != nil && len(opts.Tables) > 0 {
				query = fmt.Sprintf(columnsQueryArgsNoCodec, nArgs(len(realm.Schemas)), nArgs(len(opts.Tables)))
			}
		} else {
			query = fmt.Sprintf(columnsQueryNoNullableNoCodec, nArgs(len(realm.Schemas)))
			if opts != nil && len(opts.Tables) > 0 {
				query = fmt.Sprintf(columnsQueryArgsNoNullableNoCodec, nArgs(len(realm.Schemas)), nArgs(len(opts.Tables)))
			}
		}
		rows, err = i.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return fmt.Errorf("clickhouse: querying columns: %w", err)
	}
	defer rows.Close()
	hasNullable := strings.Contains(query, "is_nullable")
	hasCodec := strings.Contains(query, "compression_codec")
	for rows.Next() {
		var (
			name, table, typ, defaultKind, defaultExpr, comment sql.NullString
			codec                                               sql.NullString
			isNullable                                          sql.NullInt64
			db                                                  string
		)
		switch {
		case hasNullable && hasCodec:
			if err := rows.Scan(&db, &table, &name, &typ, &defaultKind, &defaultExpr, &isNullable, &comment, &codec); err != nil {
				return fmt.Errorf("clickhouse: scanning columns: %w", err)
			}
		case hasNullable:
			if err := rows.Scan(&db, &table, &name, &typ, &defaultKind, &defaultExpr, &isNullable, &comment); err != nil {
				return fmt.Errorf("clickhouse: scanning columns: %w", err)
			}
		case hasCodec:
			if err := rows.Scan(&db, &table, &name, &typ, &defaultKind, &defaultExpr, &comment, &codec); err != nil {
				return fmt.Errorf("clickhouse: scanning columns: %w", err)
			}
		default:
			if err := rows.Scan(&db, &table, &name, &typ, &defaultKind, &defaultExpr, &comment); err != nil {
				return fmt.Errorf("clickhouse: scanning columns: %w", err)
			}
		}
		if !sqlx.ValidString(name) || !sqlx.ValidString(table) {
			continue
		}
		s, ok := realm.Schema(db)
		if !ok {
			return fmt.Errorf("clickhouse: schema %q was not found in realm", db)
		}
		var (
			owner schema.Object
			col   *schema.Column
		)
		if t, ok := s.Table(table.String); ok {
			owner = t
		} else if v, ok := s.View(table.String); ok {
			owner = v
		} else {
			continue
		}
		typeStr := strings.TrimSpace(typ.String)
		baseType, nullable := unwrapNullableTopLevel(typeStr)
		if hasNullable && isNullable.Valid && isNullable.Int64 == 1 {
			nullable = true
		}
		parsed, err := ParseType(baseType)
		if err != nil {
			return err
		}
		col = &schema.Column{
			Name: name.String,
			Type: &schema.ColumnType{
				Raw:  baseType,
				Type: parsed,
				Null: nullable,
			},
		}
		if sqlx.ValidString(defaultKind) && sqlx.ValidString(defaultExpr) {
			switch strings.ToUpper(defaultKind.String) {
			case "DEFAULT":
				col.Default = &schema.RawExpr{X: defaultExpr.String}
			case "ALIAS", "MATERIALIZED":
				col.Attrs = append(col.Attrs, &schema.GeneratedExpr{
					Expr: defaultExpr.String,
					Type: strings.ToUpper(defaultKind.String),
				})
			}
		}
		if sqlx.ValidString(comment) {
			col.Attrs = append(col.Attrs, &schema.Comment{Text: comment.String})
		}
		if sqlx.ValidString(codec) {
			cv := normalizeCodec(codec.String)
			if cv != "" {
				col.Attrs = append(col.Attrs, &ColumnCodec{V: cv})
			}
		}
		switch o := owner.(type) {
		case *schema.Table:
			o.Columns = append(o.Columns, col)
		case *schema.View:
			o.Columns = append(o.Columns, col)
		}
	}
	return nil
}

func (i *inspect) indexes(ctx context.Context, realm *schema.Realm, opts *schema.InspectOptions) error {
	var (
		args      []any
		withTable bool
	)
	if opts == nil {
		opts = &schema.InspectOptions{}
	}
	for _, s := range realm.Schemas {
		args = append(args, s.Name)
	}
	if opts != nil && len(opts.Tables) > 0 {
		for _, t := range opts.Tables {
			args = append(args, t)
		}
		withTable = true
	}
	cols := "type_full, type, expr, granularity"
	query := indexesQuery(cols, len(realm.Schemas), len(opts.Tables), withTable)
	rows, err := i.QueryContext(ctx, query, args...)
	hasTypeFull := true
	if err != nil && missingColumn(err, "type_full") {
		cols = "type, expr, granularity"
		query = indexesQuery(cols, len(realm.Schemas), len(opts.Tables), withTable)
		rows, err = i.QueryContext(ctx, query, args...)
		hasTypeFull = false
	}
	if err != nil && missingColumn(err, "expr") {
		cols = strings.Replace(cols, "expr", "expression", 1)
		query = indexesQuery(cols, len(realm.Schemas), len(opts.Tables), withTable)
		rows, err = i.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return fmt.Errorf("clickhouse: querying indexes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name, typ, expr sql.NullString
			typeFull        sql.NullString
			gran            sql.NullInt64
			db, table       string
		)
		if hasTypeFull {
			if err := rows.Scan(&db, &table, &name, &typeFull, &typ, &expr, &gran); err != nil {
				return fmt.Errorf("clickhouse: scanning indexes: %w", err)
			}
		} else if err := rows.Scan(&db, &table, &name, &typ, &expr, &gran); err != nil {
			return fmt.Errorf("clickhouse: scanning indexes: %w", err)
		}
		if !sqlx.ValidString(name) || !sqlx.ValidString(expr) {
			continue
		}
		s, ok := realm.Schema(db)
		if !ok {
			return fmt.Errorf("clickhouse: schema %q was not found in realm", db)
		}
		t, ok := s.Table(table)
		if !ok {
			continue
		}
		idx := &schema.Index{
			Name:  name.String,
			Table: t,
			Parts: []*schema.IndexPart{{X: &schema.RawExpr{X: expr.String}}},
		}
		typVal := typ.String
		if sqlx.ValidString(typeFull) {
			typVal = typeFull.String
		}
		if strings.TrimSpace(typVal) != "" {
			idx.Attrs = append(idx.Attrs, &IndexType{T: typVal})
		}
		if gran.Valid {
			idx.Attrs = append(idx.Attrs, &IndexGranularity{V: int(gran.Int64)})
		}
		t.Indexes = append(t.Indexes, idx)
	}
	return nil
}

func parseViewDef(create string) string {
	create = strings.TrimSpace(create)
	if create == "" {
		return ""
	}
	upper := strings.ToUpper(create)
	idx := strings.Index(upper, " AS ")
	if idx == -1 {
		return ""
	}
	return strings.TrimSpace(create[idx+4:])
}

func parseMaterializedView(create string) (to, def string) {
	create = strings.TrimSpace(create)
	if create == "" {
		return "", ""
	}
	upper := strings.ToUpper(create)
	asIdx := strings.Index(upper, " AS ")
	if asIdx == -1 {
		return "", ""
	}
	def = strings.TrimSpace(create[asIdx+4:])
	head := create[:asIdx]
	upperHead := upper[:asIdx]
	toIdx := strings.Index(upperHead, " TO ")
	if toIdx == -1 {
		return "", def
	}
	to = strings.TrimSpace(head[toIdx+4:])
	return to, def
}

func nArgs(n int) string {
	if n == 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

func indexesQuery(cols string, schemaArgs, tableArgs int, withTable bool) string {
	if withTable {
		return fmt.Sprintf(indexesQueryArgsBase, cols, nArgs(schemaArgs), nArgs(tableArgs))
	}
	return fmt.Sprintf(indexesQueryBase, cols, nArgs(schemaArgs))
}

func missingColumn(err error, col string) bool {
	return err != nil && strings.Contains(err.Error(), "Missing columns") && strings.Contains(err.Error(), "'"+col+"'")
}

const (
	schemasQuery                      = "SELECT name FROM system.databases"
	currentSchemaQuery                = "SELECT currentDatabase()"
	schemasQueryArgs                  = "SELECT name FROM system.databases WHERE name %s"
	tablesQuery                       = "SELECT database, name, engine, engine_full, create_table_query FROM system.tables WHERE database IN (%s)"
	tablesQueryArgs                   = "SELECT database, name, engine, engine_full, create_table_query FROM system.tables WHERE database IN (%s) AND name IN (%s)"
	columnsQuery                      = "SELECT database, table, name, type, default_kind, default_expression, is_nullable, comment, compression_codec FROM system.columns WHERE database IN (%s) ORDER BY database, table, position"
	columnsQueryArgs                  = "SELECT database, table, name, type, default_kind, default_expression, is_nullable, comment, compression_codec FROM system.columns WHERE database IN (%s) AND table IN (%s) ORDER BY database, table, position"
	columnsQueryNoNullable            = "SELECT database, table, name, type, default_kind, default_expression, comment, compression_codec FROM system.columns WHERE database IN (%s) ORDER BY database, table, position"
	columnsQueryArgsNoNullable        = "SELECT database, table, name, type, default_kind, default_expression, comment, compression_codec FROM system.columns WHERE database IN (%s) AND table IN (%s) ORDER BY database, table, position"
	columnsQueryNoCodec               = "SELECT database, table, name, type, default_kind, default_expression, is_nullable, comment FROM system.columns WHERE database IN (%s) ORDER BY database, table, position"
	columnsQueryArgsNoCodec           = "SELECT database, table, name, type, default_kind, default_expression, is_nullable, comment FROM system.columns WHERE database IN (%s) AND table IN (%s) ORDER BY database, table, position"
	columnsQueryNoNullableNoCodec     = "SELECT database, table, name, type, default_kind, default_expression, comment FROM system.columns WHERE database IN (%s) ORDER BY database, table, position"
	columnsQueryArgsNoNullableNoCodec = "SELECT database, table, name, type, default_kind, default_expression, comment FROM system.columns WHERE database IN (%s) AND table IN (%s) ORDER BY database, table, position"
	indexesQueryBase                  = "SELECT database, table, name, %s FROM system.data_skipping_indices WHERE database IN (%s)"
	indexesQueryArgsBase              = "SELECT database, table, name, %s FROM system.data_skipping_indices WHERE database IN (%s) AND table IN (%s)"
)
