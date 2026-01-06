// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package schemahcl

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
)

const (
	nodesAttr    = "num_nodes"
	nodesKeyAttr = "nodes_key"
	nodeVar      = "atlas_node"
	nodesVar     = "atlas_nodes"
)

func applyNodesFilter(ctx *hcl.EvalContext, block *hclsyntax.Block, attrs []*Attr) ([]*Attr, bool, error) {
	var (
		nodesAttrVal *Attr
		nodesKeyVal  *Attr
		filtered     = make([]*Attr, 0, len(attrs))
	)
	for _, attr := range attrs {
		switch attr.K {
		case nodesAttr:
			nodesAttrVal = attr
		case nodesKeyAttr:
			nodesKeyVal = attr
		default:
			filtered = append(filtered, attr)
		}
	}
	if nodesAttrVal == nil {
		return attrs, true, nil
	}
	count, err := nodesAttrVal.Int()
	if err != nil {
		return nil, false, attrErr(nodesAttrVal, "num_nodes must be a number")
	}
	if count <= 0 {
		return filtered, true, nil
	}
	node, nodes, err := nodeFilterVars(ctx)
	if err != nil {
		return nil, false, err
	}
	if len(nodes) == 0 {
		return nil, false, attrErr(nodesAttrVal, "nodes selection requires var.atlas_nodes to be non-empty")
	}
	if count >= len(nodes) {
		return filtered, true, nil
	}
	key := blockKey(block)
	if nodesKeyVal != nil {
		key, err = nodesKeyVal.String()
		if err != nil {
			return nil, false, attrErr(nodesKeyVal, "nodes_key must be a string")
		}
	}
	ok, err := inNodeSubset(node, nodes, key, count)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return filtered, true, nil
}

func nodeFilterVars(ctx *hcl.EvalContext) (string, []string, error) {
	if ctx == nil || ctx.Variables == nil {
		return "", nil, fmt.Errorf("nodes selection requires var.atlas_node and var.atlas_nodes to be set")
	}
	vars, ok := ctx.Variables[RefVar]
	if !ok || vars.IsNull() || !vars.Type().IsObjectType() {
		return "", nil, fmt.Errorf("nodes selection requires var.atlas_node and var.atlas_nodes to be set")
	}
	nodeV, ok := objectAttr(vars, nodeVar)
	if !ok {
		return "", nil, fmt.Errorf("nodes selection requires var.atlas_node to be set")
	}
	nodesV, ok := objectAttr(vars, nodesVar)
	if !ok {
		return "", nil, fmt.Errorf("nodes selection requires var.atlas_nodes to be set")
	}
	var node string
	if err := gocty.FromCtyValue(nodeV, &node); err != nil {
		return "", nil, fmt.Errorf("nodes selection requires var.atlas_node to be a string")
	}
	nodes, err := nodesList(nodesV)
	if err != nil {
		return "", nil, err
	}
	return node, nodes, nil
}

func objectAttr(v cty.Value, name string) (cty.Value, bool) {
	if !v.Type().IsObjectType() || !v.Type().HasAttribute(name) {
		return cty.NilVal, false
	}
	return v.GetAttr(name), true
}

func nodesList(v cty.Value) ([]string, error) {
	if v.IsNull() {
		return nil, fmt.Errorf("nodes selection requires var.atlas_nodes to be a list of strings")
	}
	switch t := v.Type(); {
	case t.IsTupleType(), t.IsListType(), t.IsSetType():
		nodes := make([]string, 0, v.LengthInt())
		for it := v.ElementIterator(); it.Next(); {
			_, ev := it.Element()
			var s string
			if err := gocty.FromCtyValue(ev, &s); err != nil {
				return nil, fmt.Errorf("nodes selection requires var.atlas_nodes to be a list of strings")
			}
			nodes = append(nodes, s)
		}
		if t.IsSetType() {
			sort.Strings(nodes)
		}
		return nodes, nil
	case t == cty.String:
		var s string
		if err := gocty.FromCtyValue(v, &s); err != nil {
			return nil, fmt.Errorf("nodes selection requires var.atlas_nodes to be a list of strings")
		}
		return []string{s}, nil
	default:
		return nil, fmt.Errorf("nodes selection requires var.atlas_nodes to be a list of strings")
	}
}

func inNodeSubset(node string, nodes []string, key string, count int) (bool, error) {
	idx := -1
	for i, n := range nodes {
		if n == node {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false, fmt.Errorf("nodes selection requires var.atlas_node to exist in var.atlas_nodes")
	}
	start := int(hashKey(key) % uint64(len(nodes)))
	for i := 0; i < count; i++ {
		if (start+i)%len(nodes) == idx {
			return true, nil
		}
	}
	return false, nil
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func blockKey(block *hclsyntax.Block) string {
	if block == nil {
		return ""
	}
	if len(block.Labels) == 0 {
		return block.Type
	}
	parts := make([]string, 0, len(block.Labels)+1)
	parts = append(parts, block.Type)
	parts = append(parts, block.Labels...)
	return strings.Join(parts, ".")
}

func attrErr(attr *Attr, msg string) error {
	if attr != nil && attr.Range() != nil {
		return fmt.Errorf("%s: %s", attr.Range(), msg)
	}
	return fmt.Errorf("%s", msg)
}
