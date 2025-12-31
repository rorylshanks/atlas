// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"ariga.io/atlas/schemahcl"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlclient"
)

type (
	// Driver represents a ClickHouse driver for inspecting database schemas,
	// generating diffs between schema elements and applying migrations changes.
	Driver struct {
		*conn
		schema.Differ
		schema.Inspector
		migrate.PlanApplier
	}

	// database connection and its information.
	conn struct {
		schema.ExecQuerier
		// The schema defined in the URL path, if any.
		schema string
	}
)

var _ interface {
	migrate.StmtScanner
	schema.TypeParseFormatter
} = (*Driver)(nil)

// DriverName holds the name used for registration.
const DriverName = "clickhouse"

func init() {
	sqlclient.Register(
		DriverName,
		sqlclient.OpenerFunc(opener),
		sqlclient.RegisterDriverOpener(Open),
		sqlclient.RegisterCodec(codec, codec),
		sqlclient.RegisterURLParser(parser{}),
	)
}

func opener(_ context.Context, u *url.URL) (*sqlclient.Client, error) {
	ur := parser{}.ParseURL(u)
	db, err := sql.Open(DriverName, ur.DSN)
	if err != nil {
		return nil, err
	}
	drv, err := Open(db)
	if err != nil {
		if cerr := db.Close(); cerr != nil {
			err = fmt.Errorf("%w: %v", err, cerr)
		}
		return nil, err
	}
	drv.(*Driver).schema = ur.Schema
	return &sqlclient.Client{
		Name:   DriverName,
		DB:     db,
		URL:    ur,
		Driver: drv,
	}, nil
}

// Open opens a new ClickHouse driver.
func Open(db schema.ExecQuerier) (migrate.Driver, error) {
	c := &conn{ExecQuerier: db}
	return &Driver{
		conn:        c,
		Differ:      &sqlx.Diff{DiffDriver: &diff{}},
		Inspector:   &inspect{c},
		PlanApplier: &planApply{c},
	}, nil
}

// StmtBuilder is a helper method used to build statements with ClickHouse formatting.
func (*Driver) StmtBuilder(opts migrate.PlanOptions) *sqlx.Builder {
	b := &sqlx.Builder{QuoteOpening: '`', QuoteClosing: '`'}
	if opts.SchemaQualifier != nil {
		b.Schema = opts.SchemaQualifier
	}
	b.Indent = opts.Indent
	return b
}

// ScanStmts implements migrate.StmtScanner.
func (*Driver) ScanStmts(input string) ([]*migrate.Stmt, error) {
	return migrate.Stmts(input)
}

// ParseType converts the raw database type to its schema.Type representation.
func (*Driver) ParseType(raw string) (schema.Type, error) {
	return ParseType(raw)
}

// FormatType converts a schema type to its column form in the database.
func (*Driver) FormatType(t schema.Type) (string, error) {
	return FormatType(t)
}

// NormalizeRealm returns the normal representation of the given database.
func (d *Driver) NormalizeRealm(ctx context.Context, r *schema.Realm) (*schema.Realm, error) {
	return (&sqlx.DevDriver{Driver: d}).NormalizeRealm(ctx, r)
}

// NormalizeSchema returns the normal representation of the given schema.
func (d *Driver) NormalizeSchema(ctx context.Context, s *schema.Schema) (*schema.Schema, error) {
	return (&sqlx.DevDriver{Driver: d}).NormalizeSchema(ctx, s)
}

// Lock implements the schema.Locker interface. ClickHouse does not support advisory locks,
// so this is a no-op lock used for serialization by Atlas clients.
func (*Driver) Lock(context.Context, string, time.Duration) (schema.UnlockFunc, error) {
	return func() error { return nil }, nil
}

// Snapshot implements migrate.Snapshoter.
func (d *Driver) Snapshot(ctx context.Context) (migrate.RestoreFunc, error) {
	if d.schema != "" {
		s, err := d.InspectSchema(ctx, d.schema, nil)
		if err != nil && !schema.IsNotExistError(err) {
			return nil, err
		}
		if s != nil {
			if len(s.Tables) > 0 {
				return nil, &migrate.NotCleanError{
					State:  schema.NewRealm(s),
					Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0].Name, s.Name),
				}
			}
			return d.SchemaRestoreFunc(s), nil
		}
	}
	realm, err := d.InspectRealm(ctx, nil)
	if err != nil {
		return nil, err
	}
	if len(realm.Schemas) > 0 {
		for _, s := range realm.Schemas {
			if len(s.Tables) > 0 {
				return nil, &migrate.NotCleanError{
					State:  realm,
					Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0].Name, s.Name),
				}
			}
		}
	}
	return d.RealmRestoreFunc(realm), nil
}

// SchemaRestoreFunc returns a function that restores the given schema to its desired state.
func (d *Driver) SchemaRestoreFunc(desired *schema.Schema) migrate.RestoreFunc {
	return func(ctx context.Context) error {
		current, err := d.InspectSchema(ctx, desired.Name, nil)
		if err != nil {
			return err
		}
		changes, err := d.SchemaDiff(current, desired)
		if err != nil {
			return err
		}
		return d.ApplyChanges(ctx, changes)
	}
}

// RealmRestoreFunc returns a function that restores the given realm to its desired state.
func (d *Driver) RealmRestoreFunc(desired *schema.Realm) migrate.RestoreFunc {
	return func(ctx context.Context) error {
		current, err := d.InspectRealm(ctx, nil)
		if err != nil {
			return err
		}
		changes, err := d.RealmDiff(current, desired)
		if err != nil {
			return err
		}
		return d.ApplyChanges(ctx, changes)
	}
}

// CheckClean implements migrate.CleanChecker.
func (d *Driver) CheckClean(ctx context.Context, revT *migrate.TableIdent) error {
	if revT == nil {
		revT = &migrate.TableIdent{}
	}
	s, err := d.InspectSchema(ctx, "", nil)
	if err != nil && !schema.IsNotExistError(err) {
		return err
	}
	if s != nil {
		sameSchema := revT.Schema == "" || s.Name == revT.Schema
		singleRev := len(s.Tables) == 1 && s.Tables[0].Name == revT.Name
		if len(s.Tables) == 0 || (sameSchema && singleRev) {
			return nil
		}
		return &migrate.NotCleanError{
			State:  schema.NewRealm(s),
			Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0].Name, s.Name),
		}
	}
	r, err := d.InspectRealm(ctx, nil)
	if err != nil {
		return err
	}
	switch n := len(r.Schemas); {
	case n > 1:
		return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found multiple schemas: %d", len(r.Schemas))}
	case n == 1 && r.Schemas[0].Name != revT.Schema:
		return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found schema %q", r.Schemas[0].Name)}
	case n == 1 && len(r.Schemas[0].Tables) > 1:
		return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found multiple tables: %d", len(r.Schemas[0].Tables))}
	case n == 1 && len(r.Schemas[0].Tables) == 1 && r.Schemas[0].Tables[0].Name != revT.Name:
		return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found table %q", r.Schemas[0].Tables[0].Name)}
	}
	return nil
}

// ParseURL extends the standard url.URL with additional clickhouse information.
type parser struct{}

func (parser) ParseURL(u *url.URL) *sqlclient.URL {
	ur := &sqlclient.URL{URL: u}
	ur.DSN = u.String()
	if u.Path != "" && u.Path != "/" {
		ur.Schema = strings.TrimPrefix(u.Path, "/")
	}
	if ur.Schema == "" {
		if v := u.Query().Get("database"); v != "" {
			ur.Schema = v
		}
	}
	return ur
}

// sqlspecState implements the schemahcl.Evaluator interface for clickhouse.
var (
	// MarshalHCL marshals v into an Atlas HCL DDL document.
	MarshalHCL = schemahcl.MarshalerFunc(codec.MarshalSpec)
	// EvalHCL implements the schemahcl.Evaluator interface.
	EvalHCL = schemahcl.EvalFunc(codec.Eval)
)
