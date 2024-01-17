use datafusion::logical_expr::Like;
use datafusion::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
    sql::sqlparser::ast::{self, Expr as SQLExpr, Query, Select, SetExpr, TableFactor},
};

use datafusion::common::{internal_err, not_impl_err};
use datafusion::common::{Column, DFSchemaRef};
#[allow(unused_imports)]
use datafusion::logical_expr::aggregate_function;
use datafusion::logical_expr::expr::{
    Alias, BinaryExpr, Case, Cast, InList, ScalarFunction as DFScalarFunction, WindowFunction,
};
use datafusion::logical_expr::{Between, LogicalPlan, Operator};
use datafusion::prelude::Expr;

pub fn query_to_sql(plan: &LogicalPlan) -> Result<ast::Statement> {
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Distinct(_) => {
            let mut query = new_select();
            select_to_sql(plan, &mut query)?;

            Ok(ast::Statement::Query(Box::new(query)))
        }
        LogicalPlan::Dml(_) => dml_to_sql(plan),
        LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Prepare(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_) => Err(DataFusionError::NotImplemented(
            "Unsupported operator: {plan:?}".to_string(),
        )),
    }
}

fn select_to_sql(plan: &LogicalPlan, query: &mut ast::Query) -> Result<()> {
    let select: &mut Box<Select>;
    match &mut *query.body {
        SetExpr::Select(s) => {
            select = s;
        }
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Unexpected query body: {query.body:?}".to_string(),
            ))
        }
    }

    match plan {
        LogicalPlan::TableScan(scan) => {
            if select.from.is_empty() {
                select.from.push(new_table());
            }

            let cnt = select.from.len();
            match &mut select.from[cnt - 1].relation {
                TableFactor::Table { name, .. } => {
                    name.0.push(new_ident(scan.table_name.table().to_string()));
                }
                _ => return internal_err!("unexpected tableFactor"),
            }

            Ok(())
        }
        LogicalPlan::Projection(p) => {
            let mut exprs = p
                .expr
                .iter()
                .map(|e| select_item_to_sql(e, p.input.schema(), 0).unwrap())
                .collect::<Vec<_>>();
            select.projection.append(&mut exprs);

            select_to_sql(p.input.as_ref(), query)
        }
        LogicalPlan::Filter(filter) => {
            let filter_expr = expr_to_sql(&filter.predicate, filter.input.schema(), 0)?;

            select.selection = Some(filter_expr);

            select_to_sql(filter.input.as_ref(), query)
        }
        LogicalPlan::Limit(limit) => {
            if let Some(fetch) = limit.fetch {
                query.limit = Some(ast::Expr::Value(ast::Value::Number(
                    fetch.to_string(),
                    false,
                )));
            }

            select_to_sql(limit.input.as_ref(), query)
        }
        LogicalPlan::Sort(_sort) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Aggregate(_agg) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Distinct(_distinct) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Join(_join) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::SubqueryAlias(plan_alias) => {
            if select.from.is_empty() {
                select.from.push(new_table());
            }

            let cnt = select.from.len();
            match &mut select.from[cnt - 1].relation {
                TableFactor::Table { alias, .. } => {
                    *alias = Some(new_table_alias(plan_alias.alias.table().to_string()));
                }
                _ => return internal_err!("unexpected tableFactor"),
            }

            select_to_sql(plan_alias.input.as_ref(), query)
        }
        LogicalPlan::Union(_union) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Window(_window) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Extension(_) => not_impl_err!("Unsupported operator: {plan:?}"),
        _ => not_impl_err!("Unsupported operator: {plan:?}"),
    }
}

fn select_item_to_sql(
    expr: &Expr,
    schema: &DFSchemaRef,
    col_ref_offset: usize,
) -> Result<ast::SelectItem> {
    match expr {
        Expr::Alias(Alias { expr, name, .. }) => {
            let inner = expr_to_sql(expr, schema, col_ref_offset)?;

            Ok(ast::SelectItem::ExprWithAlias {
                expr: inner,
                alias: new_ident(name.to_string()),
            })
        }
        _ => {
            let inner = expr_to_sql(expr, schema, col_ref_offset)?;

            Ok(ast::SelectItem::UnnamedExpr(inner))
        }
    }
}

fn expr_to_sql(expr: &Expr, _schema: &DFSchemaRef, _col_ref_offset: usize) -> Result<SQLExpr> {
    match expr {
        Expr::InList(InList {
            expr,
            list: _,
            negated: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::ScalarFunction(DFScalarFunction { .. }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Between(Between {
            expr,
            negated: _,
            low: _,
            high: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Column(col) => col_to_sql(col),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let l = expr_to_sql(left.as_ref(), _schema, 0)?;
            let r = expr_to_sql(right.as_ref(), _schema, 0)?;
            let op = op_to_sql(op)?;

            Ok(SQLExpr::BinaryOp {
                left: Box::new(l),
                op,
                right: Box::new(r),
            })
        }
        Expr::Case(Case {
            expr,
            when_then_expr: _,
            else_expr: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Cast(Cast { expr, data_type: _ }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Literal(value) => Ok(ast::Expr::Value(scalar_to_sql(value)?)),
        Expr::Alias(Alias { expr, name: _, .. }) => expr_to_sql(expr, _schema, _col_ref_offset),
        Expr::WindowFunction(WindowFunction {
            fun: _,
            args: _,
            partition_by: _,
            order_by: _,
            window_frame: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        Expr::Like(Like {
            negated: _,
            expr,
            pattern: _,
            escape_char: _,
            case_insensitive: _,
        }) => {
            not_impl_err!("Unsupported expression: {expr:?}")
        }
        _ => not_impl_err!("Unsupported expression: {expr:?}"),
    }
}

fn op_to_sql(op: &Operator) -> Result<ast::BinaryOperator> {
    match op {
        Operator::Eq => Ok(ast::BinaryOperator::Eq),
        Operator::NotEq => Ok(ast::BinaryOperator::NotEq),
        Operator::Lt => Ok(ast::BinaryOperator::Lt),
        Operator::LtEq => Ok(ast::BinaryOperator::LtEq),
        Operator::Gt => Ok(ast::BinaryOperator::Gt),
        Operator::GtEq => Ok(ast::BinaryOperator::GtEq),
        Operator::Plus => Ok(ast::BinaryOperator::Plus),
        Operator::Minus => Ok(ast::BinaryOperator::Minus),
        Operator::Multiply => Ok(ast::BinaryOperator::Multiply),
        Operator::Divide => Ok(ast::BinaryOperator::Divide),
        Operator::Modulo => Ok(ast::BinaryOperator::Modulo),
        Operator::And => Ok(ast::BinaryOperator::And),
        Operator::Or => Ok(ast::BinaryOperator::Or),
        Operator::IsDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
        Operator::IsNotDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
        Operator::RegexMatch => Ok(ast::BinaryOperator::PGRegexMatch),
        Operator::RegexIMatch => Ok(ast::BinaryOperator::PGRegexIMatch),
        Operator::RegexNotMatch => Ok(ast::BinaryOperator::PGRegexNotMatch),
        Operator::RegexNotIMatch => Ok(ast::BinaryOperator::PGRegexNotIMatch),
        Operator::BitwiseAnd => Ok(ast::BinaryOperator::BitwiseAnd),
        Operator::BitwiseOr => Ok(ast::BinaryOperator::BitwiseOr),
        Operator::BitwiseXor => Ok(ast::BinaryOperator::BitwiseXor),
        Operator::BitwiseShiftRight => Ok(ast::BinaryOperator::PGBitwiseShiftRight),
        Operator::BitwiseShiftLeft => Ok(ast::BinaryOperator::PGBitwiseShiftLeft),
        Operator::StringConcat => Ok(ast::BinaryOperator::StringConcat),
        Operator::AtArrow => not_impl_err!("unsupported operation: {op:?}"),
        Operator::ArrowAt => not_impl_err!("unsupported operation: {op:?}"),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> Result<ast::Value> {
    match v {
        ScalarValue::Null => Ok(ast::Value::Null),
        ScalarValue::Boolean(Some(b)) => Ok(ast::Value::Boolean(b.to_owned())),
        ScalarValue::Boolean(None) => Ok(ast::Value::Null),
        ScalarValue::Float32(Some(f)) => Ok(ast::Value::Number(f.to_string(), false)),
        ScalarValue::Float32(None) => Ok(ast::Value::Null),
        ScalarValue::Float64(Some(f)) => Ok(ast::Value::Number(f.to_string(), false)),
        ScalarValue::Float64(None) => Ok(ast::Value::Null),
        ScalarValue::Decimal128(Some(_), ..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Decimal128(None, ..) => Ok(ast::Value::Null),
        ScalarValue::Decimal256(Some(_), ..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Decimal256(None, ..) => Ok(ast::Value::Null),
        ScalarValue::Int8(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int8(None) => Ok(ast::Value::Null),
        ScalarValue::Int16(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int16(None) => Ok(ast::Value::Null),
        ScalarValue::Int32(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int32(None) => Ok(ast::Value::Null),
        ScalarValue::Int64(Some(i)) => Ok(ast::Value::Number(i.to_string(), false)),
        ScalarValue::Int64(None) => Ok(ast::Value::Null),
        ScalarValue::UInt8(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt8(None) => Ok(ast::Value::Null),
        ScalarValue::UInt16(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt16(None) => Ok(ast::Value::Null),
        ScalarValue::UInt32(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt32(None) => Ok(ast::Value::Null),
        ScalarValue::UInt64(Some(ui)) => Ok(ast::Value::Number(ui.to_string(), false)),
        ScalarValue::UInt64(None) => Ok(ast::Value::Null),
        ScalarValue::Utf8(Some(str)) => Ok(ast::Value::SingleQuotedString(str.to_string())),
        ScalarValue::Utf8(None) => Ok(ast::Value::Null),
        ScalarValue::LargeUtf8(Some(str)) => Ok(ast::Value::SingleQuotedString(str.to_string())),
        ScalarValue::LargeUtf8(None) => Ok(ast::Value::Null),
        ScalarValue::Binary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Binary(None) => Ok(ast::Value::Null),
        ScalarValue::FixedSizeBinary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeBinary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeBinary(None) => Ok(ast::Value::Null),
        ScalarValue::FixedSizeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::List(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date32(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date32(None) => Ok(ast::Value::Null),
        ScalarValue::Date64(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date64(None) => Ok(ast::Value::Null),
        ScalarValue::Time32Second(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time32Second(None) => Ok(ast::Value::Null),
        ScalarValue::Time32Millisecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time32Millisecond(None) => Ok(ast::Value::Null),
        ScalarValue::Time64Microsecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time64Microsecond(None) => Ok(ast::Value::Null),
        ScalarValue::Time64Nanosecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time64Nanosecond(None) => Ok(ast::Value::Null),
        ScalarValue::TimestampSecond(Some(_ts), _) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::TimestampSecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::TimestampMillisecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampMillisecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::TimestampMicrosecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampMicrosecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::TimestampNanosecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampNanosecond(None, _) => Ok(ast::Value::Null),
        ScalarValue::IntervalYearMonth(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalYearMonth(None) => Ok(ast::Value::Null),
        ScalarValue::IntervalDayTime(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalDayTime(None) => Ok(ast::Value::Null),
        ScalarValue::IntervalMonthDayNano(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalMonthDayNano(None) => Ok(ast::Value::Null),
        ScalarValue::DurationSecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationSecond(None) => Ok(ast::Value::Null),
        ScalarValue::DurationMillisecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationMillisecond(None) => Ok(ast::Value::Null),
        ScalarValue::DurationMicrosecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationMicrosecond(None) => Ok(ast::Value::Null),
        ScalarValue::DurationNanosecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationNanosecond(None) => Ok(ast::Value::Null),
        ScalarValue::Struct(Some(_), _) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Struct(None, _) => Ok(ast::Value::Null),
        ScalarValue::Dictionary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
    }
}

fn col_to_sql(col: &Column) -> Result<ast::Expr> {
    Ok(ast::Expr::CompoundIdentifier(
        [
            col.relation.as_ref().unwrap().table().to_string(),
            col.name.to_string(),
        ]
        .iter()
        .map(|i| new_ident(i.to_string()))
        .collect(),
    ))
}

fn new_select() -> ast::Query {
    Query {
        with: None,
        body: Box::new(ast::SetExpr::Select(Box::new(ast::Select {
            distinct: None,
            top: None,
            projection: Vec::new(),
            into: None,
            from: Vec::new(),
            lateral_views: Vec::new(),
            selection: None,
            group_by: ast::GroupByExpr::Expressions(Vec::new()),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
        }))),
        order_by: Vec::new(),
        limit: None,
        limit_by: Vec::new(),
        offset: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
    }
}

fn new_table_alias(alias: String) -> ast::TableAlias {
    ast::TableAlias {
        name: new_ident(alias),
        columns: Vec::new(),
    }
}

fn new_table() -> ast::TableWithJoins {
    ast::TableWithJoins {
        relation: ast::TableFactor::Table {
            name: ast::ObjectName(Vec::new()),
            alias: None,
            args: None,
            with_hints: Vec::new(),
            version: None,
            partitions: Vec::new(),
        },
        joins: Vec::new(),
    }
}

fn new_ident(str: String) -> ast::Ident {
    ast::Ident {
        value: str,
        quote_style: Some('`'),
    }
}

fn dml_to_sql(_plan: &LogicalPlan) -> Result<ast::Statement> {
    Err(DataFusionError::NotImplemented(
        "dml unsupported".to_string(),
    ))
}
