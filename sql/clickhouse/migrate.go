// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
)

// DefaultPlan provides basic planning capabilities for ClickHouse dialects.
// Note, it is recommended to call Open, create a new Driver and use its
// migrate.PlanApplier when a database connection is available.
var DefaultPlan migrate.PlanApplier = &planApply{conn: &conn{ExecQuerier: sqlx.NoRows}}

// A planApply provides migration capabilities for schema elements.
type planApply struct{ *conn }

// PlanChanges returns a migration plan for the given schema changes.
func (p *planApply) PlanChanges(ctx context.Context, name string, changes []schema.Change, opts ...migrate.PlanOption) (*migrate.Plan, error) {
	s := &state{
		conn: p.conn,
		Plan: migrate.Plan{
			Name:          name,
			Transactional: false,
		},
	}
	for _, o := range opts {
		o(&s.PlanOptions)
	}
	if err := s.plan(ctx, changes); err != nil {
		return nil, err
	}
	if err := sqlx.SetReversible(&s.Plan); err != nil {
		return nil, err
	}
	return &s.Plan, nil
}

// ApplyChanges applies the changes on the database.
func (p *planApply) ApplyChanges(ctx context.Context, changes []schema.Change, opts ...migrate.PlanOption) error {
	return sqlx.ApplyChanges(ctx, changes, p, opts...)
}

// state represents the state of a planning.
type state struct {
	*conn
	migrate.Plan
	migrate.PlanOptions
}

func (s *state) plan(ctx context.Context, changes []schema.Change) error {
	for _, c := range changes {
		switch c := c.(type) {
		case *schema.AddSchema:
			s.addSchema(c)
		case *schema.DropSchema:
			s.dropSchema(c)
		case *schema.AddTable:
			if err := s.addTable(c); err != nil {
				return err
			}
		case *schema.DropTable:
			s.dropTable(c)
		case *schema.ModifyTable:
			if err := s.modifyTable(c); err != nil {
				return err
			}
		case *schema.AddView:
			if err := s.addView(c); err != nil {
				return err
			}
		case *schema.DropView:
			s.dropView(c)
		case *schema.ModifyView:
			if err := s.modifyView(c); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported change %T", c)
		}
	}
	return nil
}

func (s *state) addSchema(add *schema.AddSchema) {
	b := s.Build("CREATE DATABASE")
	if sqlx.Has(add.Extra, &schema.IfNotExists{}) {
		b.P("IF NOT EXISTS")
	}
	b.Ident(add.S.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Reverse: s.Build("DROP DATABASE").Ident(add.S.Name).String(),
		Comment: fmt.Sprintf("create %q schema", add.S.Name),
	})
}

func (s *state) dropSchema(drop *schema.DropSchema) {
	b := s.Build("DROP DATABASE")
	if sqlx.Has(drop.Extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	b.Ident(drop.S.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop %q schema", drop.S.Name),
	})
}

func (s *state) addTable(add *schema.AddTable) error {
	var (
		err      error
		b        = s.Build("CREATE TABLE")
		t        = add.T
		eng      Engine
		partBy   EnginePartitionBy
		orderBy  EngineOrderBy
		ttl      EngineTTL
		settings EngineSettings
		hasE     = sqlx.Has(t.Attrs, &eng)
	)
	if !hasE || strings.TrimSpace(eng.V) == "" {
		return fmt.Errorf("clickhouse: table %q is missing engine definition", t.Name)
	}
	if sqlx.Has(add.Extra, &schema.IfNotExists{}) {
		b.P("IF NOT EXISTS")
	}
	b.Table(t)
	b.WrapIndent(func(b *sqlx.Builder) {
		if e := s.tableElements(b, t); e != nil && err == nil {
			err = e
		}
	})
	b.NL()
	_ = sqlx.Has(t.Attrs, &partBy)
	_ = sqlx.Has(t.Attrs, &orderBy)
	_ = sqlx.Has(t.Attrs, &ttl)
	_ = sqlx.Has(t.Attrs, &settings)
	b.WriteString(engineClauseWithOptions(eng.V, partBy.V, orderBy.V, ttl.V, settings.V))
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Reverse: s.Build("DROP TABLE").Table(t).String(),
		Comment: fmt.Sprintf("create %q table", t.Name),
	})
	return err
}

func (s *state) dropTable(drop *schema.DropTable) {
	b := s.Build("DROP TABLE")
	if sqlx.Has(drop.Extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	b.Table(drop.T)
	if drop.T.Schema != nil {
		var v MaxTableSizeToDrop
		if sqlx.Has(drop.T.Schema.Attrs, &v) {
			b.P("SETTINGS max_table_size_to_drop=" + strconv.Itoa(v.V))
		}
	}
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop %q table", drop.T.Name),
	})
}

func (s *state) modifyTable(modify *schema.ModifyTable) error {
	if requiresTableRecreate(modify.Changes) {
		s.dropTable(&schema.DropTable{T: modify.T})
		return s.addTable(&schema.AddTable{T: modify.T})
	}
	for _, change := range modify.Changes {
		switch change := change.(type) {
		case *schema.AddColumn:
			if err := s.addColumn(modify.T, change); err != nil {
				return err
			}
		case *schema.DropColumn:
			s.dropColumn(modify.T, change)
		case *schema.AddIndex:
			if err := s.addIndex(modify.T, change); err != nil {
				return err
			}
		case *schema.DropIndex:
			s.dropIndex(modify.T, change)
		case *schema.ModifyIndex:
			s.dropIndex(modify.T, &schema.DropIndex{I: change.From})
			if err := s.addIndex(modify.T, &schema.AddIndex{I: change.To}); err != nil {
				return err
			}
		case *schema.AddAttr:
			if settings, ok := change.A.(*EngineSettings); ok {
				if err := s.applySettingsDiff(modify.T, "", settings.V, change); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("clickhouse: unsupported add attribute %T", change.A)
		case *schema.DropAttr:
			if settings, ok := change.A.(*EngineSettings); ok {
				if err := s.applySettingsDiff(modify.T, settings.V, "", change); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("clickhouse: unsupported drop attribute %T", change.A)
		case *schema.ModifyAttr:
			fromSettings, okFrom := change.From.(*EngineSettings)
			toSettings, okTo := change.To.(*EngineSettings)
			if okFrom && okTo {
				if err := s.applySettingsDiff(modify.T, fromSettings.V, toSettings.V, change); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("clickhouse: unsupported modify attribute %T", change.To)
		default:
			return fmt.Errorf("clickhouse: unsupported modify change %T", change)
		}
	}
	return nil
}

func requiresTableRecreate(changes []schema.Change) bool {
	for _, change := range changes {
		switch change := change.(type) {
		case *schema.AddAttr:
			if isEngineAttr(change.A) {
				return true
			}
		case *schema.DropAttr:
			if isEngineAttr(change.A) {
				return true
			}
		case *schema.ModifyAttr:
			if isEngineAttr(change.From) || isEngineAttr(change.To) {
				return true
			}
		}
	}
	return false
}

func (s *state) applySettingsDiff(t *schema.Table, from, to string, source schema.Change) error {
	modify, reset, err := diffSettings(from, to)
	if err != nil {
		return err
	}
	if len(modify) > 0 {
		b := s.Build("ALTER TABLE").Table(t).P("MODIFY SETTING")
		b.WriteString(strings.Join(modify, ", "))
		s.append(&migrate.Change{
			Cmd:     b.String(),
			Source:  source,
			Comment: fmt.Sprintf("modify settings on %q table", t.Name),
		})
	}
	if len(reset) > 0 {
		b := s.Build("ALTER TABLE").Table(t).P("RESET SETTING")
		b.WriteString(strings.Join(reset, ", "))
		s.append(&migrate.Change{
			Cmd:     b.String(),
			Source:  source,
			Comment: fmt.Sprintf("reset settings on %q table", t.Name),
		})
	}
	return nil
}

func diffSettings(from, to string) (modify, reset []string, err error) {
	fromMap, err := parseSettings(from)
	if err != nil {
		return nil, nil, err
	}
	toMap, err := parseSettings(to)
	if err != nil {
		return nil, nil, err
	}
	for name, toVal := range toMap {
		if fromVal, ok := fromMap[name]; !ok || strings.TrimSpace(fromVal) != strings.TrimSpace(toVal) {
			modify = append(modify, name+"="+strings.TrimSpace(toVal))
		}
	}
	for name := range fromMap {
		if _, ok := toMap[name]; !ok {
			reset = append(reset, name)
		}
	}
	sort.Strings(modify)
	sort.Strings(reset)
	return modify, reset, nil
}

func parseSettings(v string) (map[string]string, error) {
	out := make(map[string]string)
	v = strings.TrimSpace(v)
	if v == "" {
		return out, nil
	}
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx == -1 {
			return nil, fmt.Errorf("clickhouse: invalid settings %q", v)
		}
		name := strings.TrimSpace(part[:idx])
		val := strings.TrimSpace(part[idx+1:])
		if name == "" {
			return nil, fmt.Errorf("clickhouse: invalid settings %q", v)
		}
		out[name] = val
	}
	return out, nil
}

func (s *state) addColumn(t *schema.Table, add *schema.AddColumn) error {
	b := s.Build("ALTER TABLE").Table(t).P("ADD COLUMN")
	if err := s.column(b, add.C); err != nil {
		return err
	}
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Reverse: s.Build("ALTER TABLE").Table(t).P("DROP COLUMN").Ident(add.C.Name).String(),
		Comment: fmt.Sprintf("add %q column to %q", add.C.Name, t.Name),
	})
	return nil
}

func (s *state) dropColumn(t *schema.Table, drop *schema.DropColumn) {
	b := s.Build("ALTER TABLE").Table(t).P("DROP COLUMN").Ident(drop.C.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop %q column from %q", drop.C.Name, t.Name),
	})
}

func (s *state) addIndex(t *schema.Table, add *schema.AddIndex) error {
	b := s.Build("ALTER TABLE").Table(t).P("ADD")
	if err := s.indexDefinition(b, add.I); err != nil {
		return err
	}
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Reverse: s.Build("ALTER TABLE").Table(t).P("DROP INDEX").Ident(add.I.Name).String(),
		Comment: fmt.Sprintf("add %q index to %q", add.I.Name, t.Name),
	})
	return nil
}

func (s *state) dropIndex(t *schema.Table, drop *schema.DropIndex) {
	b := s.Build("ALTER TABLE").Table(t).P("DROP INDEX").Ident(drop.I.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop %q index from %q", drop.I.Name, t.Name),
	})
}

func (s *state) addView(add *schema.AddView) error {
	v := add.V
	b := s.Build(viewCreateKeyword(v))
	if sqlx.Has(add.Extra, &schema.IfNotExists{}) {
		b.P("IF NOT EXISTS")
	}
	b.View(v)
	if v.Materialized() {
		var (
			eng      Engine
			partBy   EnginePartitionBy
			orderBy  EngineOrderBy
			ttl      EngineTTL
			settings EngineSettings
			to       MaterializedViewTo
		)
		_ = sqlx.Has(v.Attrs, &to)
		if strings.TrimSpace(to.V) != "" {
			b.P("TO").WriteString(to.V)
		}
		if sqlx.Has(v.Attrs, &eng) {
			b.NL()
			_ = sqlx.Has(v.Attrs, &partBy)
			_ = sqlx.Has(v.Attrs, &orderBy)
			_ = sqlx.Has(v.Attrs, &ttl)
			_ = sqlx.Has(v.Attrs, &settings)
			b.WriteString(engineClauseWithOptions(eng.V, partBy.V, orderBy.V, ttl.V, settings.V))
		}
	}
	def := strings.TrimSpace(v.Def)
	if def == "" {
		return fmt.Errorf("clickhouse: view %q is missing definition", v.Name)
	}
	b.NL().P("AS")
	b.WriteString(def)
	reverse := dropViewStmt(s, v, nil)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Reverse: reverse,
		Comment: fmt.Sprintf("create %q view", v.Name),
	})
	return nil
}

func (s *state) dropView(drop *schema.DropView) {
	s.append(&migrate.Change{
		Cmd:     dropViewStmt(s, drop.V, drop.Extra),
		Source:  drop,
		Comment: fmt.Sprintf("drop %q view", drop.V.Name),
	})
}

func (s *state) modifyView(modify *schema.ModifyView) error {
	v := modify.To
	s.append(&migrate.Change{
		Cmd:     dropViewStmt(s, modify.From, nil),
		Source:  modify,
		Comment: fmt.Sprintf("recreate %q view", v.Name),
	})
	return s.addView(&schema.AddView{V: v})
}

func (s *state) column(b *sqlx.Builder, c *schema.Column) error {
	ft, err := FormatType(c.Type.Type)
	if err != nil {
		return fmt.Errorf("format type for column %q: %w", c.Name, err)
	}
	b.Ident(c.Name).P(wrapNullable(ft, c.Type.Null))
	if x := (schema.GeneratedExpr{}); sqlx.Has(c.Attrs, &x) {
		b.P(generatedKeyword(x.Type), x.Expr)
	} else {
		s.columnDefault(b, c)
	}
	if cc := (ColumnCodec{}); sqlx.Has(c.Attrs, &cc) && strings.TrimSpace(cc.V) != "" {
		b.P("CODEC(" + strings.TrimSpace(cc.V) + ")")
	}
	if cm := (schema.Comment{}); sqlx.Has(c.Attrs, &cm) {
		b.P("COMMENT", quoteLiteral(cm.Text))
	}
	return nil
}

func (s *state) tableElements(b *sqlx.Builder, t *schema.Table) error {
	written := 0
	for i := range t.Columns {
		if written > 0 {
			b.Comma()
		}
		b.NL()
		if err := s.column(b, t.Columns[i]); err != nil {
			return err
		}
		written++
	}
	for _, idx := range t.Indexes {
		if written > 0 {
			b.Comma()
		}
		b.NL()
		if err := s.indexDefinition(b, idx); err != nil {
			return err
		}
		written++
	}
	return nil
}

func (s *state) indexDefinition(b *sqlx.Builder, idx *schema.Index) error {
	if strings.TrimSpace(idx.Name) == "" {
		return fmt.Errorf("clickhouse: index name is required")
	}
	expr, err := indexExpr(idx)
	if err != nil {
		return err
	}
	typ, gran, err := indexAttrs(idx)
	if err != nil {
		return err
	}
	b.P("INDEX").Ident(idx.Name).P(expr).P("TYPE", typ).P("GRANULARITY", strconv.Itoa(gran))
	return nil
}

func indexExpr(idx *schema.Index) (string, error) {
	if len(idx.Parts) == 0 {
		return "", fmt.Errorf("clickhouse: index %q is missing expression", idx.Name)
	}
	parts := make([]string, 0, len(idx.Parts))
	for _, part := range idx.Parts {
		switch {
		case part.C != nil:
			parts = append(parts, part.C.Name)
		case part.X != nil:
			switch x := part.X.(type) {
			case *schema.RawExpr:
				parts = append(parts, x.X)
			case *schema.Literal:
				parts = append(parts, x.V)
			default:
				return "", fmt.Errorf("clickhouse: unsupported index expression for %q", idx.Name)
			}
		default:
			return "", fmt.Errorf("clickhouse: index %q has empty part", idx.Name)
		}
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, ", ") + ")", nil
}

func indexAttrs(idx *schema.Index) (string, int, error) {
	var (
		typ  string
		gran int
	)
	if t := (IndexType{}); sqlx.Has(idx.Attrs, &t) {
		typ = strings.TrimSpace(t.T)
	}
	if g := (IndexGranularity{}); sqlx.Has(idx.Attrs, &g) {
		gran = g.V
	}
	if typ == "" {
		return "", 0, fmt.Errorf("clickhouse: index %q is missing type", idx.Name)
	}
	if gran <= 0 {
		return "", 0, fmt.Errorf("clickhouse: index %q is missing granularity", idx.Name)
	}
	return typ, gran, nil
}

func (s *state) columnDefault(b *sqlx.Builder, c *schema.Column) {
	switch x := c.Default.(type) {
	case *schema.Literal:
		b.P("DEFAULT", literalDefault(x.V))
	case *schema.RawExpr:
		b.P("DEFAULT", x.X)
	}
}

// Build instantiates a new builder and writes the given phrase to it.
func (s *state) Build(phrases ...string) *sqlx.Builder {
	return (*Driver).StmtBuilder(nil, s.PlanOptions).P(phrases...)
}

func (s *state) append(c *migrate.Change) {
	s.Changes = append(s.Changes, c)
}

func engineClause(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	upper := strings.ToUpper(v)
	if strings.HasPrefix(upper, "ENGINE") {
		return v
	}
	return "ENGINE = " + v
}

func engineClauseWithOptions(engine, partitionBy, orderBy, ttl, settings string) string {
	base := engineClause(engine)
	if base == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(base)
	if s := strings.TrimSpace(partitionBy); s != "" {
		b.WriteString(" PARTITION BY ")
		b.WriteString(s)
	}
	if s := strings.TrimSpace(orderBy); s != "" {
		b.WriteString(" ORDER BY ")
		b.WriteString(s)
	}
	if s := strings.TrimSpace(ttl); s != "" {
		b.WriteString(" TTL ")
		b.WriteString(s)
	}
	if s := strings.TrimSpace(settings); s != "" {
		b.WriteString(" SETTINGS ")
		b.WriteString(s)
	}
	return b.String()
}


func viewCreateKeyword(v *schema.View) string {
	if v.Materialized() {
		return "CREATE MATERIALIZED VIEW"
	}
	return "CREATE VIEW"
}

func dropViewStmt(s *state, v *schema.View, extra []schema.Clause) string {
	b := s.Build("DROP")
	if sqlx.Has(extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	if v.Materialized() {
		b.P("TABLE")
	} else {
		b.P("VIEW")
	}
	b.View(v)
	return b.String()
}

func generatedKeyword(t string) string {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "ALIAS":
		return "ALIAS"
	case "MATERIALIZED":
		return "MATERIALIZED"
	case "DEFAULT":
		return "DEFAULT"
	default:
		return "DEFAULT"
	}
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
