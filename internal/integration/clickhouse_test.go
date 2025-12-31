// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"ariga.io/atlas/sql/clickhouse"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlclient"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

type chTest struct {
	*testing.T
	db   *sql.DB
	drv  migrate.Driver
	port int
	once sync.Once
}

var chTests = map[string]*chTest{
	"clickhouse": {port: 9000},
}

func chRun(t *testing.T, fn func(*chTest)) {
	for version, tt := range chTests {
		if flagVersion == "" || flagVersion == version {
			t.Run(version, func(t *testing.T) {
				tt.once.Do(func() {
					c, err := sqlclient.Open(context.Background(), fmt.Sprintf("clickhouse://localhost:%d/default", tt.port))
					require.NoError(t, err)
					tt.db, tt.drv = c.DB, c.Driver
					dbs = append(dbs, tt.db)
				})
				tt := &chTest{T: t, db: tt.db, drv: tt.drv, port: tt.port}
				fn(tt)
			})
		}
	}
}

func TestClickHouse_InspectAndApply(t *testing.T) {
	chRun(t, func(t *chTest) {
		ctx := context.Background()
		cleanup := func(name string, materialized bool) {
			if materialized {
				_, _ = t.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+name)
				return
			}
			_, _ = t.db.ExecContext(ctx, "DROP VIEW IF EXISTS "+name)
		}
		cleanupAll := func() {
			cleanup("users_mv", true)
			cleanup("users_view", false)
			_, _ = t.db.ExecContext(ctx, "DROP TABLE IF EXISTS users")
		}
		cleanupAll()
		t.Cleanup(cleanupAll)

		users := schema.NewTable("users").
			AddColumns(
				schema.NewColumn("id").SetType(&schema.UnsupportedType{T: "UInt64"}),
				schema.NewColumn("name").SetType(&schema.UnsupportedType{T: "String"}).SetNull(true),
			)
		users.AddAttrs(&clickhouse.Engine{V: "MergeTree() ORDER BY id"})

		view := schema.NewView("users_view", "SELECT id, name FROM users")
		mv := schema.NewMaterializedView("users_mv", "SELECT id FROM users")
		mv.AddAttrs(&clickhouse.Engine{V: "MergeTree() ORDER BY id"})

		err := t.drv.ApplyChanges(ctx, []schema.Change{
			&schema.AddTable{T: users},
			&schema.AddView{V: view},
			&schema.AddView{V: mv},
		})
		require.NoError(t, err)

		s, err := t.drv.InspectSchema(ctx, "default", nil)
		require.NoError(t, err)
		ut, ok := s.Table("users")
		require.True(t, ok)
		hasEngine := false
		for _, attr := range ut.Attrs {
			if e, ok := attr.(*clickhouse.Engine); ok {
				hasEngine = true
				require.Contains(t, e.V, "MergeTree")
			}
		}
		require.True(t, hasEngine)
		v, ok := s.View("users_view")
		require.True(t, ok)
		require.Equal(t, "SELECT id, name FROM users", strings.TrimSpace(v.Def))
		mv1, ok := s.Materialized("users_mv")
		require.True(t, ok)
		require.True(t, mv1.Materialized())
	})
}
