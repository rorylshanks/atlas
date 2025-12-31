// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package atlasexec_test

import (
	"testing"
	"time"

	"ariga.io/atlas/atlasexec"
	"ariga.io/atlas/sql/sqlcheck"
	"github.com/stretchr/testify/require"
)

func TestMigrateApplySummary(t *testing.T) {
	fail := "check failed"
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Second)
	apply := &atlasexec.MigrateApply{
		Start: start,
		End:   end,
		Applied: []*atlasexec.AppliedFile{
			{
				Applied: []string{"stmt1", "stmt2"},
				Checks: []*atlasexec.FileChecks{
					{
						Stmts: []*atlasexec.Check{
							{Stmt: "check ok"},
							{Stmt: "check fail", Error: &fail},
						},
					},
				},
			},
			{
				Applied: []string{"stmt3"},
				Error: &struct {
					Stmt string
					Text string
				}{
					Stmt: "stmt3",
					Text: "boom",
				},
			},
		},
	}
	got := apply.Summary("  ")
	expected := "- **2s**\n  - **1 migration ok, 1 with errors**\n  - **1 check ok, 1 failure**\n  - **2 sql statements ok, 1 with errors**"
	require.Equal(t, expected, got)
}

func TestSummaryReportDiagnosticsAndErrors(t *testing.T) {
	report := &atlasexec.SummaryReport{
		Files: []*atlasexec.FileReport{
			{
				Reports: []sqlcheck.Report{{
					Diagnostics: []sqlcheck.Diagnostic{
						{Text: "d1"},
						{Text: "d2"},
					},
				}},
			},
			{
				Reports: []sqlcheck.Report{{
					Diagnostics: []sqlcheck.Diagnostic{
						{Text: "d3"},
					},
				}},
				Error: "lint error",
			},
		},
	}
	require.Equal(t, 3, report.DiagnosticsCount())
	errs := report.Errors()
	require.Len(t, errs, 1)
	require.EqualError(t, errs[0], "lint error")
}
