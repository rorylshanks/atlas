// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import "strings"

func splitEngineFull(v string) (engine, partitionBy, orderBy, ttl, settings string) {
	s := strings.TrimSpace(v)
	if s == "" {
		return "", "", "", "", ""
	}
	s = stripEnginePrefix(s)
	upper := strings.ToUpper(s)
	type clause struct {
		kind string
		idx  int
		size int
	}
	clauses := []clause{
		{kind: "partition_by", idx: keywordIndex(upper, "PARTITION BY"), size: len("PARTITION BY")},
		{kind: "order_by", idx: keywordIndex(upper, "ORDER BY"), size: len("ORDER BY")},
		{kind: "ttl", idx: keywordIndex(upper, "TTL"), size: len("TTL")},
		{kind: "settings", idx: keywordIndex(upper, "SETTINGS"), size: len("SETTINGS")},
	}
	filtered := clauses[:0]
	for _, c := range clauses {
		if c.idx >= 0 {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return strings.TrimSpace(s), "", "", "", ""
	}
	for i := 0; i < len(filtered)-1; i++ {
		for j := i + 1; j < len(filtered); j++ {
			if filtered[j].idx < filtered[i].idx {
				filtered[i], filtered[j] = filtered[j], filtered[i]
			}
		}
	}
	engine = strings.TrimSpace(s[:filtered[0].idx])
	for i, c := range filtered {
		start := c.idx + c.size
		for start < len(s) && isSpace(s[start]) {
			start++
		}
		end := len(s)
		if i+1 < len(filtered) {
			end = filtered[i+1].idx
		}
		val := strings.TrimSpace(s[start:end])
		switch c.kind {
		case "partition_by":
			partitionBy = val
		case "order_by":
			orderBy = val
		case "ttl":
			ttl = val
		case "settings":
			settings = val
		}
	}
	return engine, partitionBy, orderBy, ttl, settings
}

func stripEnginePrefix(s string) string {
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)
	if strings.HasPrefix(upper, "ENGINE") {
		s = strings.TrimSpace(s[len("ENGINE"):])
		if strings.HasPrefix(s, "=") {
			s = strings.TrimSpace(s[1:])
		}
	}
	return s
}

func keywordIndex(s, kw string) int {
	for offset := 0; offset <= len(s); {
		idx := strings.Index(s[offset:], kw)
		if idx == -1 {
			return -1
		}
		idx += offset
		end := idx + len(kw)
		if (idx == 0 || isSpace(s[idx-1])) && (end == len(s) || isSpace(s[end])) {
			return idx
		}
		offset = idx + 1
	}
	return -1
}

func isSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func normalizeCodec(v string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		return ""
	}
	upper := strings.ToUpper(s)
	if strings.HasPrefix(upper, "CODEC") {
		s = strings.TrimSpace(s[len("CODEC"):])
		if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
			s = strings.TrimSpace(s[1 : len(s)-1])
		}
	}
	return s
}
