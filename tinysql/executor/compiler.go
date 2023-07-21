// Copyright 2015 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx sessionctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	infoSchema := infoschema.GetInfoSchema(c.Ctx)
	// 做一些合法性检查以及名字绑定
	if err := plannercore.Preprocess(c.Ctx, stmtNode, infoSchema); err != nil {
		return nil, err
	}

	// 制定查询计划，并优化
	finalPlan, names, err := planner.Optimize(ctx, c.Ctx, stmtNode, infoSchema)
	if err != nil {
		return nil, err
	}
	// 构造 executor.ExecStmt结构：这个 ExecStmt 结构持有查询计划，是后续执行的基础
	return &ExecStmt{
		InfoSchema:  infoSchema,
		Plan:        finalPlan,
		Text:        stmtNode.Text(),
		StmtNode:    stmtNode,
		Ctx:         c.Ctx,
		OutputNames: names,
	}, nil
}
