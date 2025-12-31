// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package atlasexec_test

import (
	"os/exec"
	"strings"
	"sync"
	"testing"
)

var atlasHelpCache sync.Map

func atlasHelp(t *testing.T, args ...string) string {
	t.Helper()
	key := strings.Join(args, "\x00")
	if cached, ok := atlasHelpCache.Load(key); ok {
		return cached.(string)
	}
	path, err := exec.LookPath("atlas")
	if err != nil {
		t.Skipf("atlas CLI not found: %v", err)
	}
	helpArgs := append([]string{}, args...)
	helpArgs = append(helpArgs, "--help")
	out, _ := exec.Command(path, helpArgs...).CombinedOutput()
	helpText := string(out)
	atlasHelpCache.Store(key, helpText)
	return helpText
}

func atlasHasCommand(t *testing.T, parent []string, cmd string) bool {
	t.Helper()
	out := atlasHelp(t, parent...)
	return strings.Contains(out, "\n  "+cmd)
}

func atlasHasFlag(t *testing.T, args []string, flag string) bool {
	t.Helper()
	return strings.Contains(atlasHelp(t, args...), flag)
}

func skipIfAtlasMissingCommand(t *testing.T, parent []string, cmd string) {
	t.Helper()
	if !atlasHasCommand(t, parent, cmd) {
		scope := strings.Join(parent, " ")
		if scope == "" {
			scope = "atlas"
		} else {
			scope = "atlas " + scope
		}
		t.Skipf("%s does not support %q", scope, cmd)
	}
}

func skipIfAtlasMissingFlag(t *testing.T, args []string, flag string) {
	t.Helper()
	if !atlasHasFlag(t, args, flag) {
		scope := "atlas " + strings.Join(args, " ")
		t.Skipf("%s does not support %q", scope, flag)
	}
}
