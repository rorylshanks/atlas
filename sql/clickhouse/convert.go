// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package clickhouse

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"ariga.io/atlas/sql/schema"
)

// ParseType converts the raw database type to its schema.Type representation.
func ParseType(raw string) (schema.Type, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("clickhouse: empty type")
	}
	return &schema.UnsupportedType{T: raw}, nil
}

// FormatType converts a schema.Type to its database form.
func FormatType(t schema.Type) (string, error) {
	switch t := t.(type) {
	case nil:
		return "", fmt.Errorf("clickhouse: nil type")
	case *schema.UnsupportedType:
		return t.T, nil
	case *schema.StringType:
		return withOptionalSize(t.T, t.Size), nil
	case *schema.DecimalType:
		return withOptionalPrecisionScale(t.T, t.Precision, t.Scale), nil
	case *schema.FloatType:
		return withOptionalPrecision(t.T, t.Precision), nil
	case *schema.TimeType:
		if t.Precision != nil {
			return fmt.Sprintf("%s(%d)", t.T, *t.Precision), nil
		}
		return t.T, nil
	default:
		if s, ok := typeName(t); ok {
			return s, nil
		}
		return "", fmt.Errorf("clickhouse: unsupported type %T", t)
	}
}

func withOptionalSize(name string, size int) string {
	if size > 0 {
		return fmt.Sprintf("%s(%d)", name, size)
	}
	return name
}

func withOptionalPrecision(name string, precision int) string {
	if precision > 0 {
		return fmt.Sprintf("%s(%d)", name, precision)
	}
	return name
}

func withOptionalPrecisionScale(name string, precision, scale int) string {
	switch {
	case precision > 0 && scale > 0:
		return fmt.Sprintf("%s(%d,%d)", name, precision, scale)
	case precision > 0:
		return fmt.Sprintf("%s(%d)", name, precision)
	default:
		return name
	}
}

func typeName(t schema.Type) (string, bool) {
	rv := reflect.ValueOf(t)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return "", false
	}
	field := rv.FieldByName("T")
	if field.IsValid() && field.Kind() == reflect.String {
		return field.String(), true
	}
	return "", false
}

// wrapNullable returns the Nullable() type wrapper when needed.
func wrapNullable(t string, nullable bool) string {
	if nullable {
		return "Nullable(" + t + ")"
	}
	return t
}

// unwrapNullableTopLevel unwraps Nullable(T) if it is the top-level wrapper.
func unwrapNullableTopLevel(t string) (string, bool) {
	t = strings.TrimSpace(t)
	if !strings.HasPrefix(t, "Nullable(") || !strings.HasSuffix(t, ")") {
		return t, false
	}
	inner := t[len("Nullable(") : len(t)-1]
	depth := 0
	for _, r := range inner {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return t, false
			}
		}
	}
	if depth != 0 {
		return t, false
	}
	return strings.TrimSpace(inner), true
}

// literalDefault formats a literal default expression.
func literalDefault(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return v
	}
	if (strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'")) || (strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"")) {
		return v
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}
