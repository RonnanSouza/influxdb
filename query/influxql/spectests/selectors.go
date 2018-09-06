package spectests

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxql"
)

var selectorCreateFuncs = []func(config execute.SelectorConfig) flux.OperationSpec{
	func(config execute.SelectorConfig) flux.OperationSpec {
		return &functions.FirstOpSpec{SelectorConfig: config}
	},
	func(config execute.SelectorConfig) flux.OperationSpec {
		return &functions.LastOpSpec{SelectorConfig: config}
	},
	func(config execute.SelectorConfig) flux.OperationSpec {
		return &functions.MaxOpSpec{SelectorConfig: config}
	},
	func(config execute.SelectorConfig) flux.OperationSpec {
		return &functions.MinOpSpec{SelectorConfig: config}
	},
}

func SelectorTest(fn func(selector flux.Operation) (string, *flux.Spec)) Fixture {
	_, file, line, _ := runtime.Caller(1)
	fixture := &collection{
		file: filepath.Base(file),
		line: line,
	}

	for _, selectorSpecFn := range selectorCreateFuncs {
		spec := selectorSpecFn(execute.SelectorConfig{
			Column: execute.DefaultValueColLabel,
		})
		op := flux.Operation{
			ID:   flux.OperationID(fmt.Sprintf("%s0", spec.Kind())),
			Spec: spec,
		}

		fixture.Add(fn(op))
	}
	return fixture
}

func init() {
	RegisterFixture(
		SelectorTest(func(selector flux.Operation) (stmt string, spec *flux.Spec) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu`, selector.Spec.Kind()),
				&flux.Spec{
					Operations: []*flux.Operation{
						{
							ID: "from0",
							Spec: &functions.FromOpSpec{
								BucketID: bucketID,
							},
						},
						{
							ID: "range0",
							Spec: &functions.RangeOpSpec{
								Start:    flux.Time{Absolute: time.Unix(0, influxql.MinTime)},
								Stop:     flux.Time{Absolute: time.Unix(0, influxql.MaxTime)},
								TimeCol:  execute.DefaultTimeColLabel,
								StartCol: execute.DefaultStartColLabel,
								StopCol:  execute.DefaultStopColLabel,
							},
						},
						{
							ID: "filter0",
							Spec: &functions.FilterOpSpec{
								Fn: &semantic.FunctionExpression{
									Params: []*semantic.FunctionParam{
										{Key: &semantic.Identifier{Name: "r"}},
									},
									Body: &semantic.LogicalExpression{
										Operator: ast.AndOperator,
										Left: &semantic.BinaryExpression{
											Operator: ast.EqualOperator,
											Left: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_measurement",
											},
											Right: &semantic.StringLiteral{
												Value: "cpu",
											},
										},
										Right: &semantic.BinaryExpression{
											Operator: ast.EqualOperator,
											Left: &semantic.MemberExpression{
												Object: &semantic.IdentifierExpression{
													Name: "r",
												},
												Property: "_field",
											},
											Right: &semantic.StringLiteral{
												Value: "value",
											},
										},
									},
								},
							},
						},
						{
							ID: "group0",
							Spec: &functions.GroupOpSpec{
								By: []string{"_measurement", "_start"},
							},
						},
						&selector,
						{
							ID: "map0",
							Spec: &functions.MapOpSpec{
								Fn: &semantic.FunctionExpression{
									Params: []*semantic.FunctionParam{{
										Key: &semantic.Identifier{Name: "r"},
									}},
									Body: &semantic.ObjectExpression{
										Properties: []*semantic.Property{
											{
												Key: &semantic.Identifier{Name: "_time"},
												Value: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "r",
													},
													Property: "_time",
												},
											},
											{
												Key: &semantic.Identifier{Name: string(selector.Spec.Kind())},
												Value: &semantic.MemberExpression{
													Object: &semantic.IdentifierExpression{
														Name: "r",
													},
													Property: "_value",
												},
											},
										},
									},
								},
								MergeKey: true,
							},
						},
						{
							ID: "yield0",
							Spec: &functions.YieldOpSpec{
								Name: "0",
							},
						},
					},
					Edges: []flux.Edge{
						{Parent: "from0", Child: "range0"},
						{Parent: "range0", Child: "filter0"},
						{Parent: "filter0", Child: "group0"},
						{Parent: "group0", Child: selector.ID},
						{Parent: selector.ID, Child: "map0"},
						{Parent: "map0", Child: "yield0"},
					},
					Now: Now(),
				}
		}),
	)
}
