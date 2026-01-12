// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package cmdapi

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"text/template"
	"time"
	"unicode"

	"ariga.io/atlas/cmd/atlas/internal/cloudapi"
	"ariga.io/atlas/cmd/atlas/internal/cmdext"
	"ariga.io/atlas/cmd/atlas/internal/cmdlog"
	"ariga.io/atlas/cmd/atlas/internal/cmdstate"
	cmdmigrate "ariga.io/atlas/cmd/atlas/internal/migrate"
	"ariga.io/atlas/cmd/atlas/internal/migratelint"
	"ariga.io/atlas/schemahcl"
	"ariga.io/atlas/sql/clickhouse"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlcheck"
	"ariga.io/atlas/sql/sqlclient"

	"github.com/spf13/cobra"
)

func init() {
	schemaCmd := schemaCmd()
	schemaCmd.AddCommand(
		schemaApplyCmd(),
		schemaCleanCmd(),
		schemaDiffCmd(),
		schemaFmtCmd(),
		schemaInspectCmd(),
		unsupportedCommand("schema", "test"),
		unsupportedCommand("schema", "plan"),
		unsupportedCommand("schema", "push"),
	)
	Root.AddCommand(schemaCmd)
	migrateCmd := migrateCmd()
	migrateCmd.AddCommand(
		migrateApplyCmd(),
		migrateDiffCmd(),
		migrateHashCmd(),
		migrateImportCmd(),
		migrateLintCmd(),
		migrateNewCmd(),
		migrateSetCmd(),
		migrateStatusCmd(),
		migrateValidateCmd(),
		unsupportedCommand("migrate", "checkpoint"),
		unsupportedCommand("migrate", "down"),
		unsupportedCommand("migrate", "rebase"),
		unsupportedCommand("migrate", "rm"),
		unsupportedCommand("migrate", "edit"),
		unsupportedCommand("migrate", "push"),
		unsupportedCommand("migrate", "test"),
	)
	Root.AddCommand(migrateCmd)
}

// unsupportedCommand create a stub command that reports
// the command is not supported by this build.
func unsupportedCommand(cmd, sub string) *cobra.Command {
	s := unsupportedMessage(cmd, sub)
	c := &cobra.Command{
		Hidden: true,
		Use:    fmt.Sprintf("%s is not supported by this build", sub),
		Short:  s,
		Long:   s,
		RunE: RunE(func(*cobra.Command, []string) error {
			return AbortErrorf("%s", s)
		}),
	}
	c.SetHelpTemplate(s + "\n")
	return c
}

// unsupportedMessage returns a message informing the user that the command
// or one of its options are not supported. For example:
//
// unsupportedMessage("migrate", "checkpoint")
// unsupportedMessage("schema", "apply --plan")
func unsupportedMessage(cmd, sub string) string {
	return fmt.Sprintf(
		`'atlas %s %s' is not supported by the community version.

To install the non-community version of Atlas, use the following command:

	curl -sSf https://atlasgo.sh | sh

Or, visit the website to see all installation options:

	https://atlasgo.io/docs#installation
`,
		cmd, sub,
	)
}

type (
	// Project represents an atlas.hcl project config file.
	Project struct {
		Envs  []*Env `spec:"env"`  // List of environments
		Lint  *Lint  `spec:"lint"` // Optional global lint policy
		Diff  *Diff  `spec:"diff"` // Optional global diff policy
		Test  *Test  `spec:"test"` // Optional test configuration
		cloud *cmdext.AtlasConfig
	}
)

const (
	envSkipUpgradeSuggestions = "ATLAS_NO_UPGRADE_SUGGESTIONS"
	oneWeek                   = 7 * 24 * time.Hour
)

// maySuggestUpgrade informs the user about the limitations of the community edition to stderr
// at most once a week. The user can disable this message by setting the ATLAS_NO_UPGRADE_SUGGESTIONS
// environment variable.
func maySuggestUpgrade(cmd *cobra.Command) {
	if os.Getenv(envSkipUpgradeSuggestions) != "" || testing.Testing() {
		return
	}
	state := cmdstate.File[LocalState]{Name: localStateFile}
	prev, err := state.Read()
	if err != nil {
		return
	}
	if time.Since(prev.UpgradeSuggested) < oneWeek {
		return
	}
	s := `Notice: This Atlas edition lacks support for features such as checkpoints,
testing, down migrations, and more. Additionally, advanced database objects such as views, 
triggers, and stored procedures are not supported. To read more: https://atlasgo.io/community-edition

To install the non-community version of Atlas, use the following command:

	curl -sSf https://atlasgo.sh | sh

Or, visit the website to see all installation options:

	https://atlasgo.io/docs#installation

`
	_ = cmdlog.WarnOnce(cmd.ErrOrStderr(), cmdlog.ColorCyan(s))
	prev.UpgradeSuggested = time.Now()
	_ = state.Write(prev)
}

// migrateLintSetFlags allows setting extra flags for the 'migrate lint' command.
func migrateLintSetFlags(*cobra.Command, *migrateLintFlags) {}

// migrateLintRun is the run command for 'migrate lint'.
func migrateLintRun(cmd *cobra.Command, _ []string, flags migrateLintFlags, env *Env) error {
	dev, err := sqlclient.Open(cmd.Context(), flags.devURL)
	if err != nil {
		return err
	}
	defer dev.Close()
	dir, err := cmdmigrate.Dir(cmd.Context(), flags.dirURL, false)
	if err != nil {
		return err
	}
	var detect migratelint.ChangeDetector
	switch {
	case flags.latest == 0 && flags.gitBase == "":
		return fmt.Errorf("--%s or --%s is required", flagLatest, flagGitBase)
	case flags.latest > 0 && flags.gitBase != "":
		return fmt.Errorf("--%s and --%s are mutually exclusive", flagLatest, flagGitBase)
	case flags.latest > 0:
		detect = migratelint.LatestChanges(dir, int(flags.latest))
	case flags.gitBase != "":
		detect, err = migratelint.NewGitChangeDetector(
			dir,
			migratelint.WithWorkDir(flags.gitDir),
			migratelint.WithBase(flags.gitBase),
			migratelint.WithMigrationsPath(dir.(interface{ Path() string }).Path()),
		)
		if err != nil {
			return err
		}
	}
	format := migratelint.DefaultTemplate
	if f := flags.logFormat; f != "" {
		format, err = template.New("format").Funcs(migratelint.TemplateFuncs).Parse(f)
		if err != nil {
			return fmt.Errorf("parse format: %w", err)
		}
	}
	az, err := sqlcheck.AnalyzerFor(dev.Name, env.Lint.Remain())
	if err != nil {
		return err
	}
	r := &migratelint.Runner{
		Dev:            dev,
		Dir:            dir,
		ChangeDetector: detect,
		ReportWriter: &migratelint.TemplateWriter{
			T: format,
			W: cmd.OutOrStdout(),
		},
		Analyzers: az,
	}
	err = r.Run(cmd.Context())
	// Print the error in case it was not printed before.
	cmd.SilenceErrors = errors.As(err, &migratelint.SilentError{})
	cmd.SilenceUsage = cmd.SilenceErrors
	return err
}

func migrateDiffRun(cmd *cobra.Command, args []string, flags migrateDiffFlags, env *Env) error {
	if flags.dryRun {
		return errors.New("'--dry-run' is not supported in the community version")
	}
	ctx := cmd.Context()
	dev, err := sqlclient.Open(ctx, flags.devURL)
	if err != nil {
		return err
	}
	defer dev.Close()
	// Acquire a lock.
	unlock, err := dev.Lock(ctx, "atlas_migrate_diff", flags.lockTimeout)
	if err != nil {
		return fmt.Errorf("acquiring database lock: %w", err)
	}
	// If unlocking fails notify the user about it.
	defer func() { cobra.CheckErr(unlock()) }()
	// Open the migration directory.
	u, err := url.Parse(flags.dirURL)
	if err != nil {
		return err
	}
	dir, err := cmdmigrate.DirURL(ctx, u, false)
	if err != nil {
		return err
	}
	if flags.edit {
		l, ok := dir.(*migrate.LocalDir)
		if !ok {
			return fmt.Errorf("--edit flag supports only atlas directories, but got: %T", dir)
		}
		dir = &editDir{l}
	}
	var name, indent string
	if len(args) > 0 {
		name = args[0]
	}
	f, err := cmdmigrate.Formatter(u)
	if err != nil {
		return err
	}
	if f, indent, err = mayIndent(u, f, flags.format); err != nil {
		return err
	}
	diffOpts := diffOptions(cmd, env)
	// If there is a state-loader that requires a custom
	// 'migrate diff' handling, offload it the work.
	if d, ok := cmdext.States.Differ(flags.desiredURLs); ok {
		err := d.MigrateDiff(ctx, &cmdext.MigrateDiffOptions{
			To:      flags.desiredURLs,
			Name:    name,
			Indent:  indent,
			Dir:     dir,
			Dev:     dev,
			Options: diffOpts,
		})
		return maskNoPlan(cmd, err)
	}
	// Get a state reader for the desired state.
	desired, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    flags.desiredURLs,
		dev:     dev,
		client:  dev,
		schemas: flags.schemas,
		vars:    env.Vars(),
	})
	if err != nil {
		return err
	}
	defer desired.Close()
	opts := []migrate.PlannerOption{
		migrate.PlanFormat(f),
		migrate.PlanWithIndent(indent),
		migrate.PlanWithDiffOptions(diffOpts...),
	}
	if dev.URL.Schema != "" {
		// Disable tables qualifier in schema-mode.
		opts = append(opts, migrate.PlanWithSchemaQualifier(flags.qualifier))
	}
	// Plan the changes and create a new migration file.
	pl := migrate.NewPlanner(dev.Driver, dir, opts...)
	plan, err := func() (*migrate.Plan, error) {
		if dev.URL.Schema != "" {
			return pl.PlanSchema(ctx, name, desired.StateReader)
		}
		return pl.Plan(ctx, name, desired.StateReader)
	}()
	var cerr *migrate.NotCleanError
	switch {
	case errors.As(err, &cerr) && dev.URL.Schema == "" && desired.Schema != "":
		return fmt.Errorf("dev database is not clean (%s). Add a schema to the URL to limit the scope of the connection", cerr.Reason)
	case err != nil:
		return maskNoPlan(cmd, err)
	default:
		return pl.WritePlan(plan)
	}
}

// schemaApplyRunE is the community version of the 'atlas schema apply' command.
func schemaApplyRunE(cmd *cobra.Command, _ []string, flags *schemaApplyFlags) error {
	switch {
	case flags.edit:
		return AbortErrorf("%s", unsupportedMessage("schema", "apply --edit"))
	case flags.planURL != "":
		return AbortErrorf("%s", unsupportedMessage("schema", "apply --plan"))
	case GlobalFlags.SelectedEnv == "":
		env, err := selectEnv(cmd)
		if err != nil {
			return err
		}
		return schemaApplyRun(cmd, *flags, env)
	default:
		_, envs, err := EnvByName(cmd, GlobalFlags.SelectedEnv, GlobalFlags.Vars)
		if err != nil {
			return err
		}
		if len(envs) == 1 {
			if err := setSchemaEnvFlags(cmd, envs[0]); err != nil {
				return err
			}
			return schemaApplyRun(cmd, *flags, envs[0])
		}
		if flags.autoApprove || flags.logFormat != "" {
			return cmdEnvsRun(envs, setSchemaEnvFlags, cmd, func(env *Env) error {
				return schemaApplyRun(cmd, *flags, env)
			})
		}
		return schemaApplyRunMulti(cmd, flags, envs)
	}
}

func schemaApplyRun(cmd *cobra.Command, flags schemaApplyFlags, env *Env) error {
	var (
		err    error
		ctx    = cmd.Context()
		dev    *sqlclient.Client
		format = cmdlog.SchemaPlanTemplate
	)
	logInspectStart(cmd, flags.url)
	if err = flags.check(env); err != nil {
		return err
	}
	if v := flags.logFormat; v != "" {
		if !flags.dryRun && !flags.autoApprove {
			return errors.New(`--log and --format can only be used with --dry-run or --auto-approve`)
		}
		if format, err = template.New("format").Funcs(cmdlog.ApplyTemplateFuncs).Parse(v); err != nil {
			return fmt.Errorf("parse log format: %w", err)
		}
	}
	if flags.devURL != "" {
		if dev, err = sqlclient.Open(ctx, flags.devURL); err != nil {
			return err
		}
		defer dev.Close()
	}
	from, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    []string{flags.url},
		schemas: flags.schemas,
		exclude: flags.exclude,
	})
	if err != nil {
		return err
	}
	defer from.Close()
	client, ok := from.Closer.(*sqlclient.Client)
	if !ok {
		return errors.New("--url must be a database connection")
	}
	to, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    flags.toURLs,
		dev:     dev,
		client:  client,
		schemas: flags.schemas,
		exclude: flags.exclude,
		vars:    env.Vars(),
	})
	if err != nil {
		return err
	}
	defer to.Close()
	to = wrapStateReaderNodeLimits(to, envNodeName(env))
	diff, err := computeDiff(ctx, client, from, to, diffOptions(cmd, env)...)
	if err != nil {
		return err
	}
	if len(flags.include) > 0 {
		diff.changes = filterTableChangesByInclude(diff.changes, flags.include)
	}
	diff.changes = orderChangesForApply(client, diff.changes)
	logInspectDone(cmd, flags.url)
	maySuggestUpgrade(cmd)
	// Returning at this stage should
	// not trigger the help message.
	cmd.SilenceUsage = true
	switch changes := diff.changes; {
	case len(changes) == 0:
		cmd.Printf("%sNo changes.\n", hostPrefix(hostFromURL(flags.url)))
		return nil
	case flags.logFormat != "" && flags.autoApprove:
		var (
			applied int
			plan    *migrate.Plan
			cause   *cmdlog.StmtError
			out     = cmd.OutOrStdout()
		)
		if plan, err = client.PlanChanges(ctx, "", changes, planOptions(client)...); err != nil {
			return err
		}
		logApplyStart(cmd, env, hostFromURL(flags.url))
		if err = applyChangesWithLogging(ctx, client, changes, flags.txMode, nil); err == nil {
			applied = len(plan.Changes)
		} else if i, ok := err.(interface{ Applied() int }); ok && i.Applied() < len(plan.Changes) {
			applied, cause = i.Applied(), &cmdlog.StmtError{Stmt: plan.Changes[i.Applied()].Cmd, Text: err.Error()}
		} else {
			cause = &cmdlog.StmtError{Text: err.Error()}
		}
		err1 := format.Execute(out, cmdlog.NewSchemaApply(ctx, cmdlog.NewEnv(client, nil), plan.Changes[:applied], plan.Changes[applied:], cause))
		return errors.Join(err, err1)
	default:
		switch err := printSchemaPlan(cmd, env, client, changes, format, flags.verbose); {
		case err != nil:
			return err
		case flags.dryRun:
			return nil
		case flags.autoApprove:
			host := hostFromURL(flags.url)
			logApplyStart(cmd, env, host)
			pairs := recreatePairs(changes)
			var idx int
			return applyChangesWithLogging(ctx, client, changes, flags.txMode, func(ch schema.Change) {
				logApplyChange(cmd, host, ch, pairs, idx)
				idx++
			})
		default:
			return promptApply(cmd, flags, diff, client, dev, env)
		}
	}
}

type schemaApplyEnvPlan struct {
	env     *Env
	flags   schemaApplyFlags
	changes []schema.Change
	client  *sqlclient.Client
}

func schemaApplyRunMulti(cmd *cobra.Command, flags *schemaApplyFlags, envs []*Env) error {
	var (
		ctx      = cmd.Context()
		reset    = resetFromEnv(cmd)
		hasPlans bool
		plans    []schemaApplyEnvPlan
	)
	defer reset()
	for _, env := range envs {
		logInspectStart(cmd, env.URL)
		if err := setSchemaEnvFlags(cmd, env); err != nil {
			return err
		}
		envFlags := *flags
		plan, err := planSchemaApply(ctx, cmd, env, envFlags)
		if err != nil {
			return err
		}
		logInspectDone(cmd, env.URL)
		if len(plan.changes) > 0 {
			hasPlans = true
		}
		plans = append(plans, plan)
		reset()
	}
	for _, plan := range plans {
		switch {
		case len(plan.changes) == 0:
			cmd.Printf("%sNo changes.\n", hostPrefix(hostFromURL(plan.flags.url)))
		default:
			if err := printSchemaPlan(cmd, plan.env, plan.client, plan.changes, cmdlog.SchemaPlanTemplate, flags.verbose); err != nil {
				return err
			}
		}
		cmd.Println()
	}
	printPlanSummaryByHost(cmd, plans)
	cmd.Println()
	if !hasPlans || flags.dryRun {
		for i := range plans {
			if plans[i].client != nil {
				plans[i].client.Close()
			}
		}
		return nil
	}
	if !promptUser(cmd) {
		for i := range plans {
			if plans[i].client != nil {
				plans[i].client.Close()
			}
		}
		return nil
	}
	for i := range plans {
		p := &plans[i]
		if len(p.changes) == 0 {
			if p.client != nil {
				p.client.Close()
			}
			continue
		}
		host := hostFromURL(p.flags.url)
		logApplyStart(cmd, p.env, host)
		pairs := recreatePairs(p.changes)
		var idx int
		if err := applyChangesWithLogging(ctx, p.client, p.changes, p.flags.txMode, func(ch schema.Change) {
			logApplyChange(cmd, host, ch, pairs, idx)
			idx++
		}); err != nil {
			_ = p.client.Close()
			return err
		}
		_ = p.client.Close()
	}
	return nil
}

func planSchemaApply(ctx context.Context, cmd *cobra.Command, env *Env, flags schemaApplyFlags) (schemaApplyEnvPlan, error) {
	if err := flags.check(env); err != nil {
		return schemaApplyEnvPlan{}, err
	}
	var (
		err error
		dev *sqlclient.Client
	)
	if flags.devURL != "" {
		if dev, err = sqlclient.Open(ctx, flags.devURL); err != nil {
			return schemaApplyEnvPlan{}, err
		}
	}
	from, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    []string{flags.url},
		schemas: flags.schemas,
		exclude: flags.exclude,
	})
	if err != nil {
		if dev != nil {
			dev.Close()
		}
		return schemaApplyEnvPlan{}, err
	}
	client, ok := from.Closer.(*sqlclient.Client)
	if !ok {
		if dev != nil {
			dev.Close()
		}
		from.Close()
		return schemaApplyEnvPlan{}, errors.New("--url must be a database connection")
	}
	to, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    flags.toURLs,
		dev:     dev,
		client:  client,
		schemas: flags.schemas,
		exclude: flags.exclude,
		vars:    env.Vars(),
	})
	if err != nil {
		if dev != nil {
			dev.Close()
		}
		from.Close()
		return schemaApplyEnvPlan{}, err
	}
	to = wrapStateReaderNodeLimits(to, envNodeName(env))
	diff, err := computeDiff(ctx, client, from, to, diffOptions(cmd, env)...)
	to.Close()
	from.Close()
	if dev != nil {
		dev.Close()
	}
	if err != nil {
		return schemaApplyEnvPlan{}, err
	}
	if len(flags.include) > 0 {
		diff.changes = filterTableChangesByInclude(diff.changes, flags.include)
	}
	diff.changes = orderChangesForApply(client, diff.changes)
	if len(diff.changes) == 0 {
		return schemaApplyEnvPlan{env: env, flags: flags}, nil
	}
	applyClient, err := sqlclient.Open(ctx, flags.url)
	if err != nil {
		return schemaApplyEnvPlan{}, err
	}
	return schemaApplyEnvPlan{
		env:     env,
		flags:   flags,
		changes: diff.changes,
		client:  applyClient,
	}, nil
}

// applySchemaClean is the community-version of the 'atlas schema clean' handler.
func applySchemaClean(cmd *cobra.Command, client *sqlclient.Client, drop []schema.Change, flags schemaCleanFlags) error {
	if flags.dryRun {
		return AbortErrorf("%s", unsupportedMessage("schema", "clean --dry-run"))
	}
	if flags.logFormat != "" {
		return AbortErrorf("%s", unsupportedMessage("schema", "clean --format"))
	}
	if len(drop) == 0 {
		cmd.Println("Nothing to drop")
		return nil
	}
	if err := summary(cmd, client, drop, cmdlog.SchemaPlanTemplate); err != nil {
		return err
	}
	if flags.autoApprove || promptUser(cmd) {
		if err := client.ApplyChanges(cmd.Context(), drop); err != nil {
			return err
		}
	}
	return nil
}

func schemaDiffRun(cmd *cobra.Command, _ []string, flags schemaDiffFlags, env *Env) error {
	var (
		ctx = cmd.Context()
		c   *sqlclient.Client
	)
	if len(flags.include) > 0 {
		return AbortErrorf("%s", unsupportedMessage("schema", "diff --include"))
	}
	// We need a driver for diffing and planning. If given, dev database has precedence.
	if flags.devURL != "" {
		var err error
		c, err = sqlclient.Open(ctx, flags.devURL)
		if err != nil {
			return err
		}
		defer c.Close()
	}
	from, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    flags.fromURL,
		dev:     c,
		vars:    env.Vars(),
		schemas: flags.schemas,
		exclude: flags.exclude,
	})
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    flags.toURL,
		dev:     c,
		vars:    env.Vars(),
		schemas: flags.schemas,
		exclude: flags.exclude,
	})
	if err != nil {
		return err
	}
	defer to.Close()
	if c == nil {
		// If not both states are provided by a database connection, the call to state-reader would have returned
		// an error already. If we land in this case, we can assume both states are database connections.
		c = to.Closer.(*sqlclient.Client)
	}
	format := cmdlog.SchemaDiffTemplate
	if v := flags.format; v != "" {
		if format, err = template.New("format").Funcs(cmdlog.SchemaDiffFuncs).Parse(v); err != nil {
			return fmt.Errorf("parse log format: %w", err)
		}
	}
	to = wrapStateReaderNodeLimits(to, envNodeName(env))
	diff, err := computeDiff(ctx, c, from, to, diffOptions(cmd, env)...)
	if err != nil {
		return err
	}
	maySuggestUpgrade(cmd)
	return format.Execute(cmd.OutOrStdout(),
		cmdlog.NewSchemaDiff(ctx, c, diff.from, diff.to, diff.changes),
	)
}

func summary(cmd *cobra.Command, c *sqlclient.Client, changes []schema.Change, t *template.Template) error {
	p, err := c.PlanChanges(cmd.Context(), "", changes, planOptions(c)...)
	if err != nil {
		return err
	}
	return t.Execute(
		cmd.OutOrStdout(),
		cmdlog.NewSchemaPlan(cmd.Context(), cmdlog.NewEnv(c, nil), p.Changes, nil),
	)
}

func promptApply(cmd *cobra.Command, flags schemaApplyFlags, diff *diff, client, _ *sqlclient.Client, env *Env) error {
	if !flags.dryRun {
		printPlanSummary(cmd, env, diff.changes)
	}
	if !flags.dryRun && (flags.autoApprove || promptUser(cmd)) {
		host := hostFromURL(flags.url)
		logApplyStart(cmd, env, host)
		pairs := recreatePairs(diff.changes)
		var idx int
		return applyChangesWithLogging(cmd.Context(), client, diff.changes, flags.txMode, func(ch schema.Change) {
			logApplyChange(cmd, host, ch, pairs, idx)
			idx++
		})
	}
	return nil
}

func printTablePlanSummary(cmd *cobra.Command, changes []schema.Change) {
	adds, mods, drops := countTableChanges(changes)
	printTablePlanSummaryWithCounts(cmd, adds, mods, drops)
}

func countTableChanges(changes []schema.Change) (adds, mods, drops int) {
	for _, c := range changes {
		switch c.(type) {
		case *schema.AddTable:
			adds++
		case *schema.ModifyTable:
			mods++
		case *schema.DropTable:
			drops++
		}
	}
	return adds, mods, drops
}

func printTablePlanSummaryWithCounts(cmd *cobra.Command, adds, mods, drops int) {
	if adds == 0 && mods == 0 && drops == 0 {
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Plan: %d to add, %d to change, %d to destroy tables.\n", adds, mods, drops)
}

func filterTableChangesByInclude(changes []schema.Change, include []string) []schema.Change {
	if len(include) == 0 {
		return changes
	}
	type pat struct {
		schema string
		table  string
	}
	var patterns []pat
	for _, p := range include {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.HasPrefix(p, "table:") {
			p = strings.TrimPrefix(p, "table:")
		}
		parts := strings.Split(p, ".")
		switch len(parts) {
		case 1:
			patterns = append(patterns, pat{table: parts[0]})
		default:
			patterns = append(patterns, pat{schema: parts[0], table: parts[1]})
		}
	}
	if len(patterns) == 0 {
		return changes
	}
	matchTable := func(schemaName, tableName string) bool {
		for _, p := range patterns {
			if p.schema != "" && p.schema != "*" {
				if ok, _ := filepath.Match(p.schema, schemaName); !ok {
					continue
				}
			}
			if p.table == "" {
				continue
			}
			if p.table == "*" {
				return true
			}
			if ok, _ := filepath.Match(p.table, tableName); ok {
				return true
			}
		}
		return false
	}
	filtered := changes[:0]
	for _, c := range changes {
		switch ch := c.(type) {
		case *schema.AddTable:
			if matchTable(schemaName(ch.T), ch.T.Name) {
				filtered = append(filtered, c)
			}
		case *schema.ModifyTable:
			if matchTable(schemaName(ch.T), ch.T.Name) {
				filtered = append(filtered, c)
			}
		case *schema.DropTable:
			if matchTable(schemaName(ch.T), ch.T.Name) {
				filtered = append(filtered, c)
			}
		case *schema.RenameTable:
			if (ch.From != nil && matchTable(schemaName(ch.From), ch.From.Name)) ||
				(ch.To != nil && matchTable(schemaName(ch.To), ch.To.Name)) {
				filtered = append(filtered, c)
			}
		}
	}
	return filtered
}

func envNodeName(env *Env) string {
	if env == nil {
		return ""
	}
	attr, ok := env.Attr("node_name")
	if !ok {
		return ""
	}
	v, err := attr.String()
	if err != nil {
		return ""
	}
	return v
}

func nodeLimitsFromAttrs(attrs []schema.Attr) ([]string, bool) {
	for _, attr := range attrs {
		if limits, ok := attr.(*clickhouse.NodeLimits); ok {
			return limits.V, true
		}
	}
	return nil, false
}

func nodeNameAllowed(nodeName string, limits []string) bool {
	if nodeName == "" {
		return true
	}
	for _, limit := range limits {
		if limit == "*" || limit == nodeName {
			return true
		}
	}
	return false
}

func wrapStateReaderNodeLimits(sr *cmdext.StateReadCloser, nodeName string) *cmdext.StateReadCloser {
	if nodeName == "" || sr == nil {
		return sr
	}
	return &cmdext.StateReadCloser{
		StateReader: migrate.StateReaderFunc(func(ctx context.Context) (*schema.Realm, error) {
			realm, err := sr.ReadState(ctx)
			if err != nil {
				return nil, err
			}
			filterRealmByNodeLimits(realm, nodeName)
			return realm, nil
		}),
		Closer: sr.Closer,
		Schema: sr.Schema,
		HCL:    sr.HCL,
	}
}

func filterRealmByNodeLimits(realm *schema.Realm, nodeName string) {
	if realm == nil || nodeName == "" {
		return
	}
	for _, s := range realm.Schemas {
		tables := s.Tables[:0]
		for _, t := range s.Tables {
			if limits, ok := nodeLimitsFromAttrs(t.Attrs); ok && !nodeNameAllowed(nodeName, limits) {
				continue
			}
			tables = append(tables, t)
		}
		s.Tables = tables
		views := s.Views[:0]
		for _, v := range s.Views {
			if limits, ok := nodeLimitsFromAttrs(v.Attrs); ok && !nodeNameAllowed(nodeName, limits) {
				continue
			}
			views = append(views, v)
		}
		s.Views = views
	}
}

func schemaName(t *schema.Table) string {
	if t != nil && t.Schema != nil {
		return t.Schema.Name
	}
	return ""
}

func tableIdent(t *schema.Table) string {
	if t == nil {
		return ""
	}
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.Name
	}
	return t.Name
}

func viewIdent(v *schema.View) string {
	if v == nil {
		return ""
	}
	if v.Schema != nil && v.Schema.Name != "" {
		return v.Schema.Name + "." + v.Name
	}
	return v.Name
}

func logInspectStart(cmd *cobra.Command, rawURL string) {
	cmd.Printf("Inspecting current schema in %s\n", redactedURL(rawURL))
}

func logInspectDone(cmd *cobra.Command, rawURL string) {
	cmd.Printf("Finished inspecting schema in %s\n", redactedURL(rawURL))
}

func logApplyStart(cmd *cobra.Command, env *Env, host string) {
	if env != nil && env.Name != "" {
		cmd.Printf("%sApplying changes for env: %s\n", hostPrefix(host), env.Name)
	}
}

func logApplyChange(cmd *cobra.Command, host string, ch schema.Change, pairs map[int]recreatePair, idx int) {
	action, target, colorFn, _, ok := summarizeChange(ch, pairs, idx)
	if !ok {
		return
	}
	cmd.Printf("%s%s %s\n", hostPrefix(host), colorFn(action), target)
}

func printSchemaPlan(cmd *cobra.Command, env *Env, client *sqlclient.Client, changes []schema.Change, tpl *template.Template, verbose bool) error {
	if tpl != cmdlog.SchemaPlanTemplate || verbose {
		return summary(cmd, client, changes, tpl)
	}
	host := hostFromURL("")
	if env != nil {
		host = hostFromURL(env.URL)
	}
	printCompactPlan(cmd, host, changes)
	return nil
}

func printCompactPlan(cmd *cobra.Command, host string, changes []schema.Change) {
	pairs := recreatePairs(changes)
	gray := cmdlog.ColorGray.SprintFunc()
	for i, ch := range changes {
		if pair, ok := pairs[i]; ok {
			if pair.skip {
				continue
			}
		}
		action, target, colorFn, details, ok := summarizeChange(ch, pairs, i)
		if !ok {
			continue
		}
		cmd.Printf("%s%s %s\n", hostPrefix(host), colorFn(action), target)
		for _, d := range details {
			cmd.Printf("%s  %s\n", hostPrefix(host), gray(d))
		}
	}
}

func printPlanSummary(cmd *cobra.Command, env *Env, changes []schema.Change) {
	host := hostFromURL("")
	if env != nil {
		host = hostFromURL(env.URL)
	}
	cmd.Println("Plan:")
	if host != "" {
		cmd.Printf("  %s\n", host)
	}
	printPlanSummaryCounts(cmd, host, countChanges(changes))
}

func printPlanSummaryByHost(cmd *cobra.Command, plans []schemaApplyEnvPlan) {
	if len(plans) == 0 {
		return
	}
	type hostPlan struct {
		host  string
		count changeCounts
	}
	var (
		hostMap = make(map[string]changeCounts)
		hosts   []string
	)
	for _, plan := range plans {
		host := hostFromURL(plan.flags.url)
		if _, ok := hostMap[host]; !ok {
			hosts = append(hosts, host)
		}
		hostMap[host] = hostMap[host].add(countChanges(plan.changes))
	}
	sort.Strings(hosts)
	cmd.Println("Plan:")
	for _, host := range hosts {
		cmd.Printf("  %s\n", host)
		printPlanSummaryCounts(cmd, host, hostMap[host])
	}
}

func printPlanSummaryCounts(cmd *cobra.Command, host string, counts changeCounts) {
	indent := "    "
	if host == "" {
		indent = "  "
	}
	if counts.recreates > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "%sadd: %d, change: %d, recreate: %d, destroy: %d\n", indent, counts.adds, counts.mods, counts.recreates, counts.drops)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%sadd: %d, change: %d, destroy: %d\n", indent, counts.adds, counts.mods, counts.drops)
}

func summarizeChange(ch schema.Change, pairs map[int]recreatePair, idx int) (string, string, func(string) string, []string, bool) {
	switch c := ch.(type) {
	case *schema.AddTable:
		if pair, ok := pairs[idx]; ok && pair.recreate {
			return "RECREATE TABLE", tableIdent(c.T), colorWrap(cmdlog.ColorBlue), []string{"drop + create"}, true
		}
		return "CREATE TABLE", tableIdent(c.T), colorWrap(cmdlog.ColorGreen), nil, true
	case *schema.DropTable:
		if pair, ok := pairs[idx]; ok && pair.skip {
			return "", "", nil, nil, false
		}
		return "DROP TABLE", tableIdent(c.T), colorWrap(cmdlog.ColorRed), nil, true
	case *schema.ModifyTable:
		if hasClickhouseEngineAttrChange(c.Changes) {
			details := tableChangeDetails(c.Changes)
			details = append(details, "drop + create")
			return "RECREATE TABLE", tableIdent(c.T), colorWrap(cmdlog.ColorBlue), details, true
		}
		return "UPDATE TABLE", tableIdent(c.T), colorWrap(cmdlog.ColorYellow), tableChangeDetails(c.Changes), true
	case *schema.RenameTable:
		return "RENAME TABLE", tableIdent(c.From) + " -> " + tableIdent(c.To), colorWrap(cmdlog.ColorBlue), nil, true
	case *schema.AddView:
		if pair, ok := pairs[idx]; ok && pair.recreate {
			return viewAction("RECREATE", c.V), viewIdent(c.V), colorWrap(cmdlog.ColorBlue), []string{"drop + create"}, true
		}
		return viewAction("CREATE", c.V), viewIdent(c.V), colorWrap(cmdlog.ColorGreen), nil, true
	case *schema.DropView:
		if pair, ok := pairs[idx]; ok && pair.skip {
			return "", "", nil, nil, false
		}
		return viewAction("DROP", c.V), viewIdent(c.V), colorWrap(cmdlog.ColorRed), nil, true
	case *schema.ModifyView:
		if c.To != nil && c.To.Materialized() {
			details := viewChangeDetails(c)
			details = append(details, "drop + create")
			return viewAction("RECREATE", c.To), viewIdent(c.To), colorWrap(cmdlog.ColorBlue), details, true
		}
		return viewAction("UPDATE", c.To), viewIdent(c.To), colorWrap(cmdlog.ColorYellow), viewChangeDetails(c), true
	case *schema.RenameView:
		return viewAction("RENAME", c.From), viewIdent(c.From) + " -> " + viewIdent(c.To), colorWrap(cmdlog.ColorBlue), nil, true
	case *schema.AddSchema:
		return "CREATE DATABASE", c.S.Name, colorWrap(cmdlog.ColorGreen), nil, true
	case *schema.DropSchema:
		return "DROP DATABASE", c.S.Name, colorWrap(cmdlog.ColorRed), nil, true
	case *schema.ModifySchema:
		return "UPDATE DATABASE", c.S.Name, colorWrap(cmdlog.ColorYellow), nil, true
	default:
		return "", "", nil, nil, false
	}
}

func colorWrap(fn func(format string, a ...interface{}) string) func(string) string {
	return func(s string) string {
		return fn("%s", s)
	}
}

type recreatePair struct {
	recreate bool
	skip     bool
}

func recreatePairs(changes []schema.Change) map[int]recreatePair {
	seenAdd := make(map[string]int)
	seenDrop := make(map[string]int)
	pairs := make(map[int]recreatePair)
	for i, ch := range changes {
		switch c := ch.(type) {
		case *schema.AddTable:
			seenAdd["table:"+tableKey(c.T)] = i
		case *schema.DropTable:
			seenDrop["table:"+tableKey(c.T)] = i
		case *schema.AddView:
			seenAdd["view:"+viewKey(c.V)] = i
		case *schema.DropView:
			seenDrop["view:"+viewKey(c.V)] = i
		}
	}
	for k, addIdx := range seenAdd {
		if dropIdx, ok := seenDrop[k]; ok {
			pairs[dropIdx] = recreatePair{skip: true}
			pairs[addIdx] = recreatePair{recreate: true}
		}
	}
	return pairs
}

func tableKey(t *schema.Table) string {
	if t == nil {
		return ""
	}
	if t.Schema != nil && t.Schema.Name != "" {
		return t.Schema.Name + "." + t.Name
	}
	return t.Name
}

func viewKey(v *schema.View) string {
	if v == nil {
		return ""
	}
	if v.Schema != nil && v.Schema.Name != "" {
		return v.Schema.Name + "." + v.Name
	}
	return v.Name
}

func tableChangeDetails(changes []schema.Change) []string {
	var details []string
	for _, ch := range changes {
		switch c := ch.(type) {
		case *schema.AddColumn:
			details = append(details, "+ column "+c.C.Name+formatColumnType(c.C))
		case *schema.DropColumn:
			details = append(details, "- column "+c.C.Name)
		case *schema.ModifyColumn:
			details = append(details, "~ column "+c.To.Name+formatColumnTypeChange(c.From, c.To))
		case *schema.RenameColumn:
			details = append(details, "~ column "+c.From.Name+" -> "+c.To.Name)
		case *schema.AddIndex:
			details = append(details, "+ index "+indexName(c.I))
		case *schema.DropIndex:
			details = append(details, "- index "+indexName(c.I))
		case *schema.ModifyIndex:
			details = append(details, "~ index "+indexName(c.To))
		case *schema.RenameIndex:
			details = append(details, "~ index "+indexName(c.From)+" -> "+indexName(c.To))
		case *schema.AddPrimaryKey:
			details = append(details, "+ primary key")
		case *schema.DropPrimaryKey:
			details = append(details, "- primary key")
		case *schema.ModifyPrimaryKey:
			details = append(details, "~ primary key")
		case *schema.AddCheck:
			details = append(details, "+ check "+c.C.Name)
		case *schema.DropCheck:
			details = append(details, "- check "+c.C.Name)
		case *schema.ModifyCheck:
			details = append(details, "~ check "+c.To.Name)
		case *schema.AddForeignKey:
			details = append(details, "+ foreign key "+fkName(c.F))
		case *schema.DropForeignKey:
			details = append(details, "- foreign key "+fkName(c.F))
		case *schema.ModifyForeignKey:
			details = append(details, "~ foreign key "+fkName(c.To))
		case *schema.AddAttr:
			if key, value, ok := clickhouseTableAttr(c.A); ok {
				if v := strings.TrimSpace(value); v != "" {
					details = append(details, "+ "+key+": "+v)
				}
			}
		case *schema.DropAttr:
			if key, value, ok := clickhouseTableAttr(c.A); ok {
				if v := strings.TrimSpace(value); v != "" {
					details = append(details, "- "+key+": "+v)
				}
			}
		case *schema.ModifyAttr:
			keyFrom, valueFrom, okFrom := clickhouseTableAttr(c.From)
			keyTo, valueTo, okTo := clickhouseTableAttr(c.To)
			if okFrom && okTo && keyFrom == keyTo {
				change := formatAttrChange(strings.TrimSpace(valueFrom), strings.TrimSpace(valueTo))
				if change != "" {
					details = append(details, "~ "+keyFrom+": "+change)
				}
			}
		}
	}
	return details
}

func viewChangeDetails(ch *schema.ModifyView) []string {
	var details []string
	if ch == nil {
		return details
	}
	if ch.From != nil && ch.To != nil {
		if strings.TrimSpace(ch.From.Def) != strings.TrimSpace(ch.To.Def) {
			details = append(details, "~ def: "+diffSnippet(ch.From.Def, ch.To.Def))
		}
		fromAttrs := clickhouseViewAttrs(ch.From)
		toAttrs := clickhouseViewAttrs(ch.To)
		for _, k := range []string{"engine", "partition_by", "order_by", "ttl", "settings", "to"} {
			if strings.TrimSpace(fromAttrs[k]) != strings.TrimSpace(toAttrs[k]) {
				if change := formatViewAttrChange(k, fromAttrs[k], toAttrs[k]); change != "" {
					details = append(details, "~ "+k+": "+change)
				}
			}
		}
	}
	if len(ch.Changes) > 0 {
		details = append(details, tableChangeDetails(ch.Changes)...)
	}
	return details
}

func formatColumnType(c *schema.Column) string {
	if c == nil || c.Type == nil {
		return ""
	}
	if c.Type.Raw != "" {
		return " " + c.Type.Raw
	}
	return " " + typeName(c.Type.Type)
}

func formatColumnTypeChange(from, to *schema.Column) string {
	fromT := formatColumnType(from)
	toT := formatColumnType(to)
	if fromT == toT || toT == "" {
		return ""
	}
	if fromT == "" {
		return " " + toT
	}
	return " " + strings.TrimSpace(fromT) + " -> " + strings.TrimSpace(toT)
}

func clickhouseViewAttrs(v *schema.View) map[string]string {
	out := make(map[string]string)
	if v == nil {
		return out
	}
	for _, attr := range v.Attrs {
		switch a := attr.(type) {
		case *clickhouse.Engine:
			out["engine"] = a.V
		case *clickhouse.EnginePartitionBy:
			out["partition_by"] = a.V
		case *clickhouse.EngineOrderBy:
			out["order_by"] = a.V
		case *clickhouse.EngineTTL:
			out["ttl"] = a.V
		case *clickhouse.EngineSettings:
			out["settings"] = a.V
		case *clickhouse.MaterializedViewTo:
			out["to"] = a.V
		}
	}
	return out
}

func clickhouseTableAttr(attr schema.Attr) (string, string, bool) {
	switch a := attr.(type) {
	case *clickhouse.Engine:
		return "engine", a.V, true
	case *clickhouse.EnginePartitionBy:
		return "partition_by", a.V, true
	case *clickhouse.EngineOrderBy:
		return "order_by", a.V, true
	case *clickhouse.EngineTTL:
		return "ttl", a.V, true
	case *clickhouse.EngineSettings:
		return "settings", a.V, true
	default:
		return "", "", false
	}
}

func clickhouseRecreateAttr(attr schema.Attr) bool {
	switch attr.(type) {
	case *clickhouse.Engine, *clickhouse.EnginePartitionBy, *clickhouse.EngineOrderBy, *clickhouse.EngineTTL:
		return true
	default:
		return false
	}
}

func hasClickhouseEngineAttrChange(changes []schema.Change) bool {
	for _, ch := range changes {
		switch c := ch.(type) {
		case *schema.AddAttr:
			if clickhouseRecreateAttr(c.A) {
				return true
			}
		case *schema.DropAttr:
			if clickhouseRecreateAttr(c.A) {
				return true
			}
		case *schema.ModifyAttr:
			if clickhouseRecreateAttr(c.From) {
				return true
			}
			if clickhouseRecreateAttr(c.To) {
				return true
			}
		}
	}
	return false
}

func formatAttrChange(from, to string) string {
	switch {
	case from == "" && to == "":
		return ""
	case from == "":
		return to
	case to == "":
		return from + " -> (removed)"
	default:
		return from + " -> " + to
	}
}

func formatViewAttrChange(key, from, to string) string {
	switch key {
	case "to":
		left := formatToTarget(from)
		right := formatToTarget(to)
		if left == right {
			return ""
		}
		return left + " -> " + right
	default:
		return formatAttrChange(from, to)
	}
}

func formatToTarget(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "(none)"
	}
	name := v
	if i := strings.IndexAny(v, " ("); i != -1 {
		name = strings.TrimSpace(v[:i])
	}
	if name == "" {
		name = "(unknown)"
	}
	if cols := countTopLevelCols(v); cols > 0 {
		return fmt.Sprintf("%s (cols:%d)", name, cols)
	}
	return name
}

func countTopLevelCols(v string) int {
	start := strings.Index(v, "(")
	end := strings.LastIndex(v, ")")
	if start == -1 || end == -1 || end <= start {
		return 0
	}
	s := v[start+1 : end]
	var (
		depth  int
		commas int
		hasCol bool
	)
	for _, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				commas++
			}
		default:
			if !hasCol && !unicode.IsSpace(r) {
				hasCol = true
			}
		}
	}
	if !hasCol {
		return 0
	}
	return commas + 1
}

func diffSnippet(from, to string) string {
	a := strings.TrimSpace(from)
	b := strings.TrimSpace(to)
	p := commonPrefixLen(a, b)
	s := commonSuffixLen(a, b)
	aMid, bMid := trimDiff(a, b, p, s, 40)
	return fmt.Sprintf("%s -> %s", aMid, bMid)
}

func commonPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func commonSuffixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	return i
}

func trimDiff(a, b string, prefix, suffix, keep int) (string, string) {
	// Clamp prefix/suffix to avoid overlapping slices on identical strings.
	maxPrefix := min(prefix, min(len(a), len(b)))
	maxSuffix := min(suffix, min(len(a)-maxPrefix, len(b)-maxPrefix))
	aMid := a[maxPrefix : len(a)-maxSuffix]
	bMid := b[maxPrefix : len(b)-maxSuffix]
	return shortenMid(aMid, keep), shortenMid(bMid, keep)
}

func shortenMid(s string, keep int) string {
	s = strings.TrimSpace(s)
	if len(s) <= keep*2 {
		return s
	}
	return s[:keep] + "..." + s[len(s)-keep:]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func typeName(t schema.Type) string {
	if t == nil {
		return ""
	}
	tt := fmt.Sprintf("%T", t)
	return strings.TrimPrefix(tt, "*")
}

func indexName(i *schema.Index) string {
	if i == nil || i.Name == "" {
		return "(unnamed)"
	}
	return i.Name
}

func fkName(f *schema.ForeignKey) string {
	if f == nil || f.Symbol == "" {
		return "(unnamed)"
	}
	return f.Symbol
}

func viewAction(verb string, v *schema.View) string {
	if v != nil && v.Materialized() {
		return verb + " MATERIALIZED VIEW"
	}
	return verb + " VIEW"
}

type changeCounts struct {
	adds      int
	mods      int
	drops     int
	recreates int
}

func (c changeCounts) add(other changeCounts) changeCounts {
	return changeCounts{
		adds:      c.adds + other.adds,
		mods:      c.mods + other.mods,
		drops:     c.drops + other.drops,
		recreates: c.recreates + other.recreates,
	}
}

func countChanges(changes []schema.Change) changeCounts {
	var c changeCounts
	pairs := recreatePairs(changes)
	for i, ch := range changes {
		switch ch.(type) {
		case *schema.AddTable:
			if pair, ok := pairs[i]; ok && pair.recreate {
				c.recreates++
				continue
			}
			c.adds++
		case *schema.AddView:
			if pair, ok := pairs[i]; ok && pair.recreate {
				c.recreates++
				continue
			}
			c.adds++
		case *schema.AddSchema:
			c.adds++
		case *schema.DropTable:
			if pair, ok := pairs[i]; ok && pair.skip {
				continue
			}
			c.drops++
		case *schema.DropView:
			if pair, ok := pairs[i]; ok && pair.skip {
				continue
			}
			c.drops++
		case *schema.ModifyTable, *schema.ModifyView, *schema.ModifySchema, *schema.RenameTable, *schema.RenameView:
			switch v := ch.(type) {
			case *schema.ModifyView:
				if v.To != nil && v.To.Materialized() {
					c.recreates++
					continue
				}
			case *schema.ModifyTable:
				if hasClickhouseEngineAttrChange(v.Changes) {
					c.recreates++
					continue
				}
			}
			c.mods++
		case *schema.DropSchema:
			c.drops++
		}
	}
	return c
}

func hostFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if h := u.Hostname(); h != "" {
		return h
	}
	if u.Host != "" {
		return u.Host
	}
	return raw
}

func hostPrefix(host string) string {
	if host == "" {
		return ""
	}
	return "[" + host + "] "
}

func redactedURL(raw string) string {
	if raw == "" {
		return ""
	}
	if u, err := cloudapi.RedactedURL(raw); err == nil {
		return u
	}
	return raw
}

func applyChangesWithLogging(ctx context.Context, client *sqlclient.Client, changes []schema.Change, txMode string, logFn func(schema.Change)) error {
	opts := planOptions(client)
	if txMode == txModeNone {
		for _, ch := range changes {
			if logFn != nil {
				logFn(ch)
			}
			if err := client.ApplyChanges(ctx, []schema.Change{ch}, opts...); err != nil {
				return err
			}
		}
		return nil
	}
	tx, err := client.Tx(ctx, nil)
	if err != nil {
		return err
	}
	for _, ch := range changes {
		if logFn != nil {
			logFn(ch)
		}
		if err := tx.ApplyChanges(ctx, []schema.Change{ch}, opts...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func orderChangesForApply(client *sqlclient.Client, changes []schema.Change) []schema.Change {
	if client == nil || strings.ToLower(client.Name) != "clickhouse" || len(changes) == 0 {
		return changes
	}
	ordered := append([]schema.Change(nil), changes...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return changePriority(ordered[i]) < changePriority(ordered[j])
	})
	return ordered
}

func changePriority(c schema.Change) int {
	switch ch := c.(type) {
	case *schema.AddTable:
		if isDistributedTable(ch.T) {
			return 1
		}
		return 0
	case *schema.AddView:
		if ch.V != nil && ch.V.Materialized() {
			return 3
		}
		return 2
	default:
		return 4
	}
}

func isDistributedTable(t *schema.Table) bool {
	if t == nil {
		return false
	}
	for _, a := range t.Attrs {
		if e, ok := a.(*clickhouse.Engine); ok {
			return strings.HasPrefix(strings.TrimSpace(e.V), "Distributed(")
		}
	}
	return false
}

func maySetLoginContext(*cobra.Command, *Project) error {
	return nil
}

func setEnvs(context.Context, []*Env) {}

// specOptions are the options for the schema spec.
var specOptions []schemahcl.Option

// diffOptions returns environment-aware diff options.
func diffOptions(_ *cobra.Command, env *Env) []schema.DiffOption {
	return append(env.DiffOptions(), schema.DiffNormalized())
}

// openClient allows opening environment-aware clients.
func (*Env) openClient(ctx context.Context, u string) (*sqlclient.Client, error) {
	return sqlclient.Open(ctx, u)
}

type schemaInspectFlags struct {
	url       string   // URL of resource to inspect.
	devURL    string   // URL of the dev database.
	logFormat string   // Format of the log output.
	schemas   []string // Schemas to take into account when diffing.
	exclude   []string // List of glob patterns used to filter resources from applying (see schema.InspectOptions).
}

// schemaInspectCmd represents the 'atlas schema inspect' subcommand.
func schemaInspectCmd() *cobra.Command {
	cmd, _ := schemaInspectCmdWithFlags()
	return cmd
}

func schemaInspectCmdWithFlags() (*cobra.Command, *schemaInspectFlags) {
	var (
		env   *Env
		flags schemaInspectFlags
		cmd   = &cobra.Command{
			Use:   "inspect",
			Short: "Inspect a database and print its schema in Atlas DDL syntax.",
			Long: `'atlas schema inspect' connects to the given database and inspects its schema.
It then prints to the screen the schema of that database in Atlas DDL syntax. This output can be
saved to a file, commonly by redirecting the output to a file named with a ".hcl" suffix:

  atlas schema inspect -u "mysql://user:pass@localhost:3306/dbname" > schema.hcl

This file can then be edited and used with the` + " `atlas schema apply` " + `command to plan
and execute schema migrations against the given database. In cases where users wish to inspect
all multiple schemas in a given database (for instance a MySQL server may contain multiple named
databases), omit the relevant part from the url, e.g. "mysql://user:pass@localhost:3306/".
To select specific schemas from the databases, users may use the "--schema" (or "-s" shorthand)
flag.
	`,
			Example: `  atlas schema inspect -u "mysql://user:pass@localhost:3306/dbname"
  atlas schema inspect -u "mariadb://user:pass@localhost:3306/" --schema=schemaA,schemaB -s schemaC
  atlas schema inspect --url "postgres://user:pass@host:port/dbname?sslmode=disable"
  atlas schema inspect -u "sqlite://file:ex1.db?_fk=1"`,
			PreRunE: RunE(func(cmd *cobra.Command, args []string) (err error) {
				if env, err = selectEnv(cmd); err != nil {
					return err
				}
				return setSchemaEnvFlags(cmd, env)
			}),
			RunE: RunE(func(cmd *cobra.Command, args []string) error {
				return schemaInspectRun(cmd, args, flags, env)
			}),
		}
	)
	cmd.Flags().SortFlags = false
	addFlagURL(cmd.Flags(), &flags.url)
	addFlagDevURL(cmd.Flags(), &flags.devURL)
	addFlagSchemas(cmd.Flags(), &flags.schemas)
	addFlagExclude(cmd.Flags(), &flags.exclude)
	addFlagLog(cmd.Flags(), &flags.logFormat)
	addFlagFormat(cmd.Flags(), &flags.logFormat)
	cobra.CheckErr(cmd.MarkFlagRequired(flagURL))
	cmd.MarkFlagsMutuallyExclusive(flagLog, flagFormat)
	return cmd, &flags
}

func schemaInspectRun(cmd *cobra.Command, _ []string, flags schemaInspectFlags, env *Env) error {
	var (
		ctx = cmd.Context()
		dev *sqlclient.Client
	)
	useDev, err := readerUseDev(env, flags.url)
	if err != nil {
		return err
	}
	if flags.devURL != "" && useDev {
		if dev, err = sqlclient.Open(ctx, flags.devURL); err != nil {
			return err
		}
		defer dev.Close()
	}
	r, err := stateReader(ctx, env, &stateReaderConfig{
		urls:    []string{flags.url},
		dev:     dev,
		vars:    env.Vars(),
		schemas: flags.schemas,
		exclude: flags.exclude,
	})
	if err != nil {
		return err
	}
	defer r.Close()
	client, ok := r.Closer.(*sqlclient.Client)
	if !ok && dev != nil {
		client = dev
	}
	format := cmdlog.SchemaInspectTemplate
	if v := flags.logFormat; v != "" {
		if format, err = template.New("format").Funcs(cmdlog.InspectTemplateFuncs).Parse(v); err != nil {
			return fmt.Errorf("parse log format: %w", err)
		}
	}
	s, err := r.ReadState(ctx)
	if err != nil {
		return err
	}
	maySuggestUpgrade(cmd)
	i := cmdlog.NewSchemaInspect(ctx, client, s)
	i.URL = flags.url
	return format.Execute(cmd.OutOrStdout(), i)
}
