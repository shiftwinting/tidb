// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

type pushTopNSolver struct {
	ctx       context.Context
	allocator *idAllocator
}

func (s *pushTopNSolver) optimize(p LogicalPlan, ctx context.Context, allocator *idAllocator) (LogicalPlan, error) {
	return p.pushTopN(Sort{}.init(allocator, ctx)), nil
}

func (s *baseLogicalPlan) pushTopN(topN *Sort) LogicalPlan {
	p := s.basePlan.self.(LogicalPlan)
	for i, child := range p.Children() {
		p.Children()[i] = child.(LogicalPlan).pushTopN(Sort{}.init(topN.allocator, topN.ctx))
		p.Children()[i].SetParents(p)
	}
	return topN.setChild(p)
}

func (s *Sort) isEmpty() bool {
	return s.ExecLimit == nil && len(s.ByItems) == 0
}

func (s *Sort) isLimit() bool {
	return len(s.ByItems) == 0 && s.ExecLimit != nil
}

func (s *Sort) isTopN() bool {
	return len(s.ByItems) != 0 && s.ExecLimit != nil
}

func (s *Sort) setChild(p LogicalPlan) LogicalPlan {
	if s.isEmpty() {
		return p
	} else if s.isLimit() {
		limit := Limit{Count: s.ExecLimit.Count, Offset: s.ExecLimit.Offset}.init(s.allocator, s.ctx)
		limit.SetChildren(p)
		p.SetParents(limit)
		limit.SetSchema(p.Schema().Clone())
		return limit
	} else {
		s.SetChildren(p)
		p.SetParents(s)
		s.SetSchema(p.Schema().Clone())
		return s
	}
}

func (p *Sort) pushTopN(topN *Sort) LogicalPlan {
	if topN.isLimit() {
		p.ExecLimit = topN.ExecLimit
		// If a Limit is pushed, the Sort should be converted to topN and be pushed again.
		return p.children[0].(LogicalPlan).pushTopN(p)
	} else if topN.isEmpty() {
		// If nothing is pushed, just continue to push nothing to its child.
		return p.baseLogicalPlan.pushTopN(topN)
	} else {
		// If a TopN is pushed, this sort is useless.
		return p.children[0].(LogicalPlan).pushTopN(topN)
	}
}

func (p *Limit) pushTopN(topN *Sort) LogicalPlan {
	child := p.children[0].(LogicalPlan).pushTopN(Sort{ExecLimit: p}.init(p.allocator, p.ctx))
	return topN.setChild(child)
}

func (p *Union) pushTopN(topN *Sort) LogicalPlan {
	for i, child := range p.children {
		newTopN := Sort{}.init(p.allocator, p.ctx)
		for _, by := range topN.ByItems {
			newExpr := expression.ColumnSubstitute(by.Expr, p.schema, expression.Column2Exprs(child.Schema().Columns))
			newTopN.ByItems = append(newTopN.ByItems, &ByItems{newExpr, by.Desc})
		}
		if !topN.isEmpty() {
			newTopN.ExecLimit = &Limit{Count: topN.ExecLimit.Count}
		}
		p.children[i] = child.(LogicalPlan).pushTopN(newTopN)
		p.children[i].SetParents(p)
	}
	return topN.setChild(p)
}

func (p *Projection) pushTopN(topN *Sort) LogicalPlan {
	for _, by := range topN.ByItems {
		by.Expr = expression.ColumnSubstitute(by.Expr, p.schema, p.Exprs)
	}
	child := p.children[0].(LogicalPlan).pushTopN(topN)
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

func (p *Join) pushTopNToChild(topN *Sort, idx int) LogicalPlan {
	canPush := true
	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		if len(p.children[1-idx].Schema().ColumnsIndices(cols)) != 0 {
			canPush = false
			break
		}
	}
	newTopN := Sort{}.init(topN.allocator, topN.ctx)
	if canPush {
		if !topN.isEmpty() {
			newTopN.ExecLimit = &Limit{Count: topN.ExecLimit.Count}
		}
		newTopN.ByItems = make([]*ByItems, len(topN.ByItems))
		copy(newTopN.ByItems, topN.ByItems)
	}
	return p.children[idx].(LogicalPlan).pushTopN(newTopN)
}

func (p *Join) pushTopN(topN *Sort) LogicalPlan {
	var leftChild, rightChild LogicalPlan
	emptySort := Sort{}.init(p.allocator, p.ctx)
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin:
		leftChild = p.pushTopNToChild(topN, 0)
		rightChild = p.children[1].(LogicalPlan).pushTopN(emptySort)
	case RightOuterJoin:
		leftChild = p.children[0].(LogicalPlan).pushTopN(emptySort)
		rightChild = p.pushTopNToChild(topN, 1)
	default:
		return p.baseLogicalPlan.pushTopN(topN)
	}
	p.SetChildren(leftChild, rightChild)
	self := p.self.(LogicalPlan)
	leftChild.SetParents(self)
	rightChild.SetParents(self)
	return topN.setChild(self)
}
