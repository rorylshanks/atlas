// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package clickhouse

import (
	"fmt"
	"reflect"
	"strings"

	"ariga.io/atlas/schemahcl"
	"ariga.io/atlas/sql/internal/specutil"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlspec"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/zclconf/go-cty/cty"
)

// Codec for schemahcl.
type Codec struct {
	State *schemahcl.State
}

// Eval evaluates an Atlas DDL document into v using the input.
func (c *Codec) Eval(p *hclparse.Parser, v any, input map[string]cty.Value) error {
	return c.EvalOptions(p, v, &schemahcl.EvalOptions{Variables: input})
}

// EvalOptions decodes the HCL with the given options.
func (c *Codec) EvalOptions(p *hclparse.Parser, v any, opts *schemahcl.EvalOptions) error {
	switch v := v.(type) {
	case *schema.Realm:
		var d specutil.Doc
		if err := c.State.EvalOptions(p, &d, opts); err != nil {
			return err
		}
		if err := specutil.Scan(v,
			&specutil.ScanDoc{Schemas: d.Schemas, Tables: d.Tables, Views: d.Views, Materialized: d.Materialized},
			scanFuncs,
		); err != nil {
			return fmt.Errorf("clickhouse: failed converting to *schema.Realm: %w", err)
		}
		for _, spec := range d.Schemas {
			s, ok := v.Schema(spec.Name)
			if !ok {
				return fmt.Errorf("clickhouse: could not find schema: %q", spec.Name)
			}
			if err := convertSchemaAttrs(spec, &s.Attrs); err != nil {
				return err
			}
		}
	case *schema.Schema:
		var d specutil.Doc
		if err := c.State.EvalOptions(p, &d, opts); err != nil {
			return err
		}
		if len(d.Schemas) != 1 {
			return fmt.Errorf("clickhouse: expecting document to contain a single schema, got %d", len(d.Schemas))
		}
		r := &schema.Realm{}
		if err := specutil.Scan(r,
			&specutil.ScanDoc{Schemas: d.Schemas, Tables: d.Tables, Views: d.Views, Materialized: d.Materialized},
			scanFuncs,
		); err != nil {
			return err
		}
		if err := convertSchemaAttrs(d.Schemas[0], &r.Schemas[0].Attrs); err != nil {
			return err
		}
		*v = *r.Schemas[0]
	case schema.Schema, schema.Realm:
		return fmt.Errorf("clickhouse: Eval expects a pointer: received %[1]T, expected *%[1]T", v)
	default:
		return fmt.Errorf("clickhouse: unexpected type %T", v)
	}
	return nil
}

// MarshalSpec marshals v into an Atlas DDL document using a schemahcl.Marshaler.
func (c *Codec) MarshalSpec(v any) ([]byte, error) {
	return specutil.Marshal(v, c.State, specutil.RealmFuncs{
		Schema: schemaSpec,
	})
}

var (
	sharedSpecOptions = []schemahcl.Option{
		schemahcl.WithTypes("table.column.type", TypeRegistry.Specs()),
		schemahcl.WithTypes("view.column.type", TypeRegistry.Specs()),
		schemahcl.WithTypes("materialized.column.type", TypeRegistry.Specs()),
		schemahcl.WithScopedEnums("table.column.as.type", "DEFAULT", "MATERIALIZED", "ALIAS"),
	}
	codec = &Codec{State: schemahcl.New(sharedSpecOptions...)}

	// EvalHCLBytes is a helper that evaluates an HCL document from a byte slice.
	EvalHCLBytes = specutil.HCLBytesFunc(EvalHCL)

	scanFuncs = &specutil.ScanFuncs{
		Table: convertTable,
		View:  convertView,
	}
)

// TypeRegistry contains the supported TypeSpecs for the ClickHouse driver.
var TypeRegistry = schemahcl.NewRegistry(
	schemahcl.WithParser(func(typ string) (schema.Type, error) {
		return ParseType(typ)
	}),
	schemahcl.WithFormatter(func(t schema.Type) (string, error) {
		return FormatType(t)
	}),
	schemahcl.WithSpecs(
		schemahcl.NewTypeSpec("Bool"),
		schemahcl.NewTypeSpec("UInt8"),
		schemahcl.NewTypeSpec("UInt16"),
		schemahcl.NewTypeSpec("UInt32"),
		schemahcl.NewTypeSpec("UInt64"),
		schemahcl.NewTypeSpec("Int8"),
		schemahcl.NewTypeSpec("Int16"),
		schemahcl.NewTypeSpec("Int32"),
		schemahcl.NewTypeSpec("Int64"),
		schemahcl.NewTypeSpec("Float32"),
		schemahcl.NewTypeSpec("Float64"),
		schemahcl.NewTypeSpec("String"),
		schemahcl.NewTypeSpec("FixedString", schemahcl.WithAttributes(schemahcl.SizeTypeAttr(true))),
		schemahcl.NewTypeSpec("Date"),
		schemahcl.NewTypeSpec("DateTime"),
		schemahcl.NewTypeSpec("DateTime64", schemahcl.WithAttributes(schemahcl.PrecisionTypeAttr())),
		schemahcl.NewTypeSpec("UUID"),
		schemahcl.NewTypeSpec("Decimal", schemahcl.WithAttributes(schemahcl.PrecisionTypeAttr(), schemahcl.ScaleTypeAttr())),
		schemahcl.NewTypeSpec("Decimal32", schemahcl.WithAttributes(schemahcl.PrecisionTypeAttr())),
		schemahcl.NewTypeSpec("Decimal64", schemahcl.WithAttributes(schemahcl.PrecisionTypeAttr())),
		schemahcl.NewTypeSpec("Decimal128", schemahcl.WithAttributes(schemahcl.PrecisionTypeAttr())),
		schemahcl.NewTypeSpec("Enum8", schemahcl.WithAttributes(&schemahcl.TypeAttr{Name: "values", Kind: reflect.Slice, Required: false})),
		schemahcl.NewTypeSpec("Enum16", schemahcl.WithAttributes(&schemahcl.TypeAttr{Name: "values", Kind: reflect.Slice, Required: false})),
	),
)

// schemaSpec converts from a concrete ClickHouse schema to Atlas specification.
func schemaSpec(s *schema.Schema) (*specutil.SchemaSpec, error) {
	spec, err := specutil.FromSchema(s, &specutil.SchemaFuncs{
		Table: tableSpec,
		View:  viewSpec,
	})
	if err != nil {
		return nil, err
	}
	if v := (&MaxTableSizeToDrop{}); sqlx.Has(s.Attrs, v) {
		spec.Schema.Extra.Attrs = append(spec.Schema.Extra.Attrs, schemahcl.IntAttr("max_table_size_to_drop", v.V))
	}
	return spec, nil
}

func convertSchemaAttrs(spec *sqlspec.Schema, attrs *[]schema.Attr) error {
	if attr, ok := spec.Attr("max_table_size_to_drop"); ok {
		v, err := attr.Int64()
		if err != nil {
			return err
		}
		*attrs = append(*attrs, &MaxTableSizeToDrop{V: int(v)})
	}
	return nil
}

// convertTable converts a sqlspec.Table to a schema.Table.
func convertTable(spec *sqlspec.Table, parent *schema.Schema) (*schema.Table, error) {
	t, err := specutil.Table(spec, parent, convertColumn, specutil.PrimaryKey, convertIndex, specutil.Check)
	if err != nil {
		return nil, err
	}
	var (
		engine   string
		partBy   string
		orderBy  string
		ttl      string
		settings string
		limits   []string
	)
	if attr, ok := spec.Attr("engine"); ok {
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		engine = v
	}
	if attr, ok := spec.Attr("partition_by"); ok {
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		partBy = v
	}
	if attr, ok := spec.Attr("order_by"); ok {
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		orderBy = v
	}
	if attr, ok := spec.Attr("ttl"); ok {
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		ttl = v
	}
	if attr, ok := spec.Attr("settings"); ok {
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		settings = v
	}
	if attr, ok := spec.Attr("node_limits"); ok {
		v, err := nodeLimitsAttr(attr)
		if err != nil {
			return nil, err
		}
		limits = v
		t.AddAttrs(&NodeLimits{V: limits})
	}
	if engine != "" && partBy == "" && orderBy == "" && ttl == "" && settings == "" {
		engine, partBy, orderBy, ttl, settings = splitEngineFull(engine)
	}
	if engine != "" {
		t.AddAttrs(&Engine{V: engine})
	}
	if partBy != "" {
		t.AddAttrs(&EnginePartitionBy{V: partBy})
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
	return t, nil
}

// convertView converts a sqlspec.View to a schema.View.
func convertView(spec *sqlspec.View, parent *schema.Schema) (*schema.View, error) {
	v, err := specutil.View(
		spec, parent,
		func(c *sqlspec.Column, _ *schema.View) (*schema.Column, error) {
			return specutil.Column(c, convertColumnType)
		},
		func(i *sqlspec.Index, v *schema.View) (*schema.Index, error) {
			return nil, fmt.Errorf("unexpected view index %s.%s", v.Name, i.Name)
		},
	)
	if err != nil {
		return nil, err
	}
	var (
		engine   string
		partBy   string
		orderBy  string
		ttl      string
		settings string
		to       string
		limits   []string
	)
	if attr, ok := spec.Attr("engine"); ok {
		v1, err := attr.String()
		if err != nil {
			return nil, err
		}
		engine = v1
	}
	if attr, ok := spec.Attr("partition_by"); ok {
		v1, err := attr.String()
		if err != nil {
			return nil, err
		}
		partBy = v1
	}
	if attr, ok := spec.Attr("order_by"); ok {
		v1, err := attr.String()
		if err != nil {
			return nil, err
		}
		orderBy = v1
	}
	if attr, ok := spec.Attr("ttl"); ok {
		v1, err := attr.String()
		if err != nil {
			return nil, err
		}
		ttl = v1
	}
	if attr, ok := spec.Attr("settings"); ok {
		v1, err := attr.String()
		if err != nil {
			return nil, err
		}
		settings = v1
	}
	if attr, ok := spec.Attr("to"); ok {
		v1, err := attr.String()
		if err != nil {
			return nil, err
		}
		to = v1
	}
	if attr, ok := spec.Attr("node_limits"); ok {
		v1, err := nodeLimitsAttr(attr)
		if err != nil {
			return nil, err
		}
		limits = v1
		v.AddAttrs(&NodeLimits{V: limits})
	}
	if engine != "" && partBy == "" && orderBy == "" && ttl == "" && settings == "" {
		engine, partBy, orderBy, ttl, settings = splitEngineFull(engine)
	}
	if engine != "" {
		v.AddAttrs(&Engine{V: engine})
	}
	if partBy != "" {
		v.AddAttrs(&EnginePartitionBy{V: partBy})
	}
	if orderBy != "" {
		v.AddAttrs(&EngineOrderBy{V: orderBy})
	}
	if ttl != "" {
		v.AddAttrs(&EngineTTL{V: ttl})
	}
	if settings != "" {
		v.AddAttrs(&EngineSettings{V: settings})
	}
	if to != "" {
		v.AddAttrs(&MaterializedViewTo{V: to})
	}
	return v, nil
}

// convertColumn converts a sqlspec.Column into a schema.Column.
func convertColumn(spec *sqlspec.Column, _ *schema.Table) (*schema.Column, error) {
	c, err := specutil.Column(spec, convertColumnType)
	if err != nil {
		return nil, err
	}
	if attr, ok := spec.Attr("codec"); ok {
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		if v := normalizeCodec(v); v != "" {
			c.Attrs = append(c.Attrs, &ColumnCodec{V: v})
		}
	}
	if err := specutil.ConvertGenExpr(spec.Remain(), c, clickhouseGenType); err != nil {
		return nil, err
	}
	return c, nil
}

func convertIndex(spec *sqlspec.Index, t *schema.Table) (*schema.Index, error) {
	idx, err := specutil.Index(spec, t)
	if err != nil {
		return nil, err
	}
	if attr, ok := spec.Attr("type"); ok {
		typ, err := attr.String()
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(typ) != "" {
			idx.Attrs = append(idx.Attrs, &IndexType{T: typ})
		}
	}
	if attr, ok := spec.Attr("granularity"); ok {
		gran, err := attr.Int()
		if err != nil {
			return nil, err
		}
		if gran > 0 {
			idx.Attrs = append(idx.Attrs, &IndexGranularity{V: gran})
		}
	}
	return idx, nil
}

// convertColumnType converts a sqlspec.Column into a schema.Type.
func convertColumnType(spec *sqlspec.Column) (schema.Type, error) {
	return TypeRegistry.Type(spec.Type, spec.Extra.Attrs)
}

func nodeLimitsAttr(attr *schemahcl.Attr) ([]string, error) {
	switch attr.V.Type() {
	case cty.String:
		v, err := attr.String()
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(v) == "" {
			return []string{}, nil
		}
		return []string{v}, nil
	case cty.List(cty.String):
		return attr.Strings()
	default:
		return nil, fmt.Errorf("node_limits must be a string or list of strings, got %s", attr.V.Type().FriendlyName())
	}
}

func clickhouseGenType(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
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

// tableSpec converts from a concrete ClickHouse table to a sqlspec.Table.
func tableSpec(t *schema.Table) (*sqlspec.Table, error) {
	spec, err := specutil.FromTable(
		t,
		columnSpec,
		specutil.FromPrimaryKey,
		indexSpec,
		specutil.FromForeignKey,
		specutil.FromCheck,
	)
	if err != nil {
		return nil, err
	}
	if e := (&Engine{}); sqlx.Has(t.Attrs, e) && e.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("engine", e.V))
	}
	if p := (&EnginePartitionBy{}); sqlx.Has(t.Attrs, p) && p.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("partition_by", p.V))
	}
	if o := (&EngineOrderBy{}); sqlx.Has(t.Attrs, o) && o.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("order_by", o.V))
	}
	if x := (&EngineTTL{}); sqlx.Has(t.Attrs, x) && x.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("ttl", x.V))
	}
	if s := (&EngineSettings{}); sqlx.Has(t.Attrs, s) && s.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("settings", s.V))
	}
	if l := (&NodeLimits{}); sqlx.Has(t.Attrs, l) {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringsAttr("node_limits", l.V...))
	}
	return spec, nil
}

// viewSpec converts from a concrete ClickHouse view to a sqlspec.View.
func viewSpec(v *schema.View) (*sqlspec.View, error) {
	spec, err := specutil.FromView(v, viewColumnSpec, indexSpec)
	if err != nil {
		return nil, err
	}
	if e := (&Engine{}); sqlx.Has(v.Attrs, e) && e.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("engine", e.V))
	}
	if p := (&EnginePartitionBy{}); sqlx.Has(v.Attrs, p) && p.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("partition_by", p.V))
	}
	if o := (&EngineOrderBy{}); sqlx.Has(v.Attrs, o) && o.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("order_by", o.V))
	}
	if x := (&EngineTTL{}); sqlx.Has(v.Attrs, x) && x.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("ttl", x.V))
	}
	if s := (&EngineSettings{}); sqlx.Has(v.Attrs, s) && s.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("settings", s.V))
	}
	if t := (&MaterializedViewTo{}); sqlx.Has(v.Attrs, t) && t.V != "" {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringAttr("to", t.V))
	}
	if l := (&NodeLimits{}); sqlx.Has(v.Attrs, l) {
		spec.DefaultExtension.Extra.SetAttr(schemahcl.StringsAttr("node_limits", l.V...))
	}
	return spec, nil
}

func columnSpec(c *schema.Column, _ *schema.Table) (*sqlspec.Column, error) {
	spec, err := specutil.FromColumn(c, columnTypeSpec)
	if err != nil {
		return nil, err
	}
	if cc := (&ColumnCodec{}); sqlx.Has(c.Attrs, cc) && cc.V != "" {
		spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("codec", cc.V))
	}
	if x := (schema.GeneratedExpr{}); sqlx.Has(c.Attrs, &x) {
		spec.Extra.Children = append(spec.Extra.Children, specutil.FromGenExpr(x, clickhouseGenType))
	}
	return spec, nil
}

func viewColumnSpec(c *schema.Column, _ *schema.View) (*sqlspec.Column, error) {
	return specutil.FromColumn(c, columnTypeSpec)
}

func columnTypeSpec(t schema.Type) (*sqlspec.Column, error) {
	typ, err := TypeRegistry.Convert(t)
	if err != nil {
		return nil, err
	}
	return &sqlspec.Column{Type: typ}, nil
}

func indexSpec(idx *schema.Index) (*sqlspec.Index, error) {
	spec, err := specutil.FromIndex(idx)
	if err != nil {
		return nil, err
	}
	if i := (IndexType{}); sqlx.Has(idx.Attrs, &i) && strings.TrimSpace(i.T) != "" {
		spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("type", i.T))
	}
	if g := (IndexGranularity{}); sqlx.Has(idx.Attrs, &g) && g.V > 0 {
		spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.IntAttr("granularity", g.V))
	}
	return spec, nil
}
