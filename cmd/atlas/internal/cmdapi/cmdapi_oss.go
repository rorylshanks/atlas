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
	printEnvPlanHeader(cmd, env, flags.url)
	cmd.Println("Planning: inspect current state and load desired schema.")
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
	diff, err := computeDiff(ctx, client, from, to, diffOptions(cmd, env)...)
	if err != nil {
		return err
	}
	if len(flags.include) > 0 {
		diff.changes = filterTableChangesByInclude(diff.changes, flags.include)
	}
	diff.changes = orderChangesForApply(client, diff.changes)
	maySuggestUpgrade(cmd)
	// Returning at this stage should
	// not trigger the help message.
	cmd.SilenceUsage = true
	switch changes := diff.changes; {
	case len(changes) == 0:
		return format.Execute(cmd.OutOrStdout(), &cmdlog.SchemaApply{})
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
		logApplyStart(cmd, env)
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
		switch err := summary(cmd, client, changes, format); {
		case err != nil:
			return err
		case flags.dryRun:
			return nil
		case flags.autoApprove:
			logApplyStart(cmd, env)
			return applyChangesWithLogging(ctx, client, changes, flags.txMode, func(ch schema.Change) {
				logTableCreate(cmd, ch)
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
		totalAdd int
		totalMod int
		totalDel int
	)
	defer reset()
	for _, env := range envs {
		printEnvPlanHeader(cmd, env, env.URL)
		cmd.Println("Planning: inspect current state and load desired schema.")
		if err := setSchemaEnvFlags(cmd, env); err != nil {
			return err
		}
		envFlags := *flags
		plan, err := planSchemaApply(ctx, cmd, env, envFlags)
		if err != nil {
			return err
		}
		if len(plan.changes) > 0 {
			hasPlans = true
		}
		plans = append(plans, plan)
		reset()
	}
	for _, plan := range plans {
		printEnvPlanHeader(cmd, plan.env, plan.flags.url)
		adds, mods, drops := countTableChanges(plan.changes)
		totalAdd += adds
		totalMod += mods
		totalDel += drops
		printTablePlanSummaryWithCounts(cmd, adds, mods, drops)
		switch {
		case len(plan.changes) == 0:
			cmd.Println("No changes.")
		default:
			if err := summary(cmd, plan.client, plan.changes, cmdlog.SchemaPlanTemplate); err != nil {
				return err
			}
		}
		cmd.Println()
	}
	printTablePlanSummaryWithCounts(cmd, totalAdd, totalMod, totalDel)
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
		logApplyStart(cmd, p.env)
		if err := applyChangesWithLogging(ctx, p.client, p.changes, p.flags.txMode, func(ch schema.Change) {
			logTableCreate(cmd, ch)
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

func printEnvPlanHeader(cmd *cobra.Command, env *Env, url string) {
	cmd.Printf("Env: %s\n", env.Name)
	if url == "" {
		return
	}
	if u, err := cloudapi.RedactedURL(url); err == nil {
		cmd.Printf("URL: %s\n", u)
	}
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
		printTablePlanSummary(cmd, diff.changes)
	}
	if !flags.dryRun && (flags.autoApprove || promptUser(cmd)) {
		logApplyStart(cmd, env)
		return applyChangesWithLogging(cmd.Context(), client, diff.changes, flags.txMode, func(ch schema.Change) {
			logTableCreate(cmd, ch)
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

func logApplyStart(cmd *cobra.Command, env *Env) {
	if env != nil && env.Name != "" {
		cmd.Printf("Applying changes for env: %s\n", env.Name)
	}
}

func logTableCreate(cmd *cobra.Command, ch schema.Change) {
	if add, ok := ch.(*schema.AddTable); ok {
		cmd.Printf("Creating table: %s\n", tableIdent(add.T))
	}
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
