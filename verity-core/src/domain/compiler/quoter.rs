// verity-core/src/domain/compiler/quoter.rs
use sqlparser::ast::{
    Expr, ObjectName, ObjectNamePart, Query, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct UniversalQuoter;

impl UniversalQuoter {
    /// Transforme une requête brute en SQL cité ("field_name") compatible partout.
    pub fn quote_identifiers(sql: &str) -> Result<String, anyhow::Error> {
        let dialect = GenericDialect {}; // On commence par un parsing générique
        let ast = Parser::parse_sql(&dialect, sql)?;

        let mut transformed_ast = ast;

        for stmt in &mut transformed_ast {
            Self::process_statement(stmt);
        }

        let result = transformed_ast
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        tracing::debug!("Quoted SQL: {}", result);
        Ok(result)
    }

    fn process_statement(stmt: &mut Statement) {
        match stmt {
            Statement::Query(query) => Self::process_query(query),
            _ => {}
        }
    }

    fn process_query(query: &mut Query) {
        // 1. Gérer les CTE (WITH clause)
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                // On cite l'alias du CTE
                cte.alias.name.quote_style = Some('"');
                Self::process_query(&mut cte.query);
            }
        }

        // 2. Gérer le corps de la requête (Select ou SetOperation)
        Self::process_set_expr(&mut query.body);

        // 3. Gérer Order By
        if let Some(order_by) = &mut query.order_by {
            match &mut order_by.kind {
                sqlparser::ast::OrderByKind::Expressions(exprs) => {
                    for ob in exprs {
                        Self::process_expr(&mut ob.expr);
                    }
                }
                _ => {}
            }
        }

        // 4. Gérer Limit
        if let Some(limit_clause) = &mut query.limit_clause {
            match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(l) = limit {
                        Self::process_expr(l);
                    }
                    if let Some(o) = offset {
                        Self::process_expr(&mut o.value);
                    }
                }
                sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    Self::process_expr(offset);
                    Self::process_expr(limit);
                }
            }
        }
    }

    fn process_set_expr(set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                Self::process_select(select);
            }
            SetExpr::SetOperation { left, right, .. } => {
                Self::process_set_expr(left);
                Self::process_set_expr(right);
            }
            SetExpr::Query(subquery) => Self::process_query(subquery),
            _ => {}
        }
    }

    fn process_select(select: &mut sqlparser::ast::Select) {
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    Self::process_expr(expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    Self::process_expr(expr);
                    // Force le quoting de l'alias : alias -> "alias"
                    alias.quote_style = Some('"');
                }
                SelectItem::QualifiedWildcard(kind, _options) => match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        Self::process_object_name(name)
                    }
                    SelectItemQualifiedWildcardKind::Expr(expr) => Self::process_expr(expr),
                },
                _ => {}
            }
        }

        for table in &mut select.from {
            Self::process_table_factor(&mut table.relation);
            for join in &mut table.joins {
                Self::process_table_factor(&mut join.relation);
                match &mut join.join_operator {
                    sqlparser::ast::JoinOperator::Join(constraint)
                    | sqlparser::ast::JoinOperator::Inner(constraint)
                    | sqlparser::ast::JoinOperator::Left(constraint)
                    | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                    | sqlparser::ast::JoinOperator::Right(constraint)
                    | sqlparser::ast::JoinOperator::RightOuter(constraint)
                    | sqlparser::ast::JoinOperator::FullOuter(constraint)
                    | sqlparser::ast::JoinOperator::CrossJoin(constraint)
                    | sqlparser::ast::JoinOperator::Semi(constraint)
                    | sqlparser::ast::JoinOperator::LeftSemi(constraint)
                    | sqlparser::ast::JoinOperator::RightSemi(constraint)
                    | sqlparser::ast::JoinOperator::Anti(constraint)
                    | sqlparser::ast::JoinOperator::LeftAnti(constraint)
                    | sqlparser::ast::JoinOperator::RightAnti(constraint)
                    | sqlparser::ast::JoinOperator::StraightJoin(constraint) => match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => Self::process_expr(expr),
                        sqlparser::ast::JoinConstraint::Using(idents) => {
                            for ident in idents.iter_mut() {
                                Self::process_object_name(ident);
                            }
                        }
                        _ => {}
                    },
                    sqlparser::ast::JoinOperator::AsOf {
                        match_condition,
                        constraint,
                    } => {
                        Self::process_expr(match_condition);
                        match constraint {
                            sqlparser::ast::JoinConstraint::On(expr) => Self::process_expr(expr),
                            sqlparser::ast::JoinConstraint::Using(idents) => {
                                for ident in idents.iter_mut() {
                                    Self::process_object_name(ident);
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some(selection) = &mut select.selection {
            Self::process_expr(selection);
        }

        match &mut select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    Self::process_expr(expr);
                }
            }
            sqlparser::ast::GroupByExpr::All(_) => {}
        }

        if let Some(having) = &mut select.having {
            Self::process_expr(having);
        }

        if let Some(qualify) = &mut select.qualify {
            Self::process_expr(qualify);
        }
    }

    fn process_table_factor(tf: &mut TableFactor) {
        match tf {
            TableFactor::Table { name, alias, .. } => {
                Self::process_object_name(name);
                if let Some(a) = alias {
                    a.name.quote_style = Some('"');
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                Self::process_query(subquery);
                if let Some(a) = alias {
                    a.name.quote_style = Some('"');
                }
            }
            _ => {}
        }
    }

    fn process_object_name(name: &mut ObjectName) {
        // Liste d'exclusion pour ne pas citer les fonctions natives ANSI communes
        let sql_builtin_functions = [
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "CAST",
            "COALESCE",
            "NOW",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "UPPER",
            "LOWER",
            "REPLACE",
            "REGEXP_REPLACE",
            "SHA256",
            "CONCAT",
            "ABS",
            "ROUND",
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "LAG",
            "LEAD",
            "FIRST_VALUE",
            "LAST_VALUE",
            "NTH_VALUE",
            "NTILE",
            "PERCENT_RANK",
            "CUME_DIST",
        ];

        // CORRECTION: Valid naming parts
        for part in name.0.iter_mut() {
            match part {
                ObjectNamePart::Identifier(ident) => {
                    let val_upper = ident.value.to_uppercase();

                    // Ne jamais citer si déjà cité ou si fonction native
                    if ident.quote_style.is_none()
                        && !sql_builtin_functions.contains(&val_upper.as_str())
                    {
                        // Ne pas citer les wildcards
                        if !["*"].contains(&val_upper.as_str()) {
                            ident.quote_style = Some('"');
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn process_expr(expr: &mut Expr) {
        match expr {
            Expr::Identifier(ident) => {
                ident.quote_style = Some('"');
            }
            Expr::CompoundIdentifier(idents) => {
                for ident in idents {
                    ident.quote_style = Some('"');
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::process_expr(left.as_mut());
                Self::process_expr(right.as_mut());
            }
            Expr::UnaryOp { expr, .. } => {
                Self::process_expr(expr.as_mut());
            }
            Expr::Function(func) => {
                Self::process_object_name(&mut func.name);
                Self::process_function_arguments(&mut func.args);
                Self::process_function_arguments(&mut func.parameters);
                if let Some(over) = &mut func.over {
                    Self::process_window_type(over);
                }
                if let Some(filter) = &mut func.filter {
                    Self::process_expr(filter.as_mut());
                }
            }
            Expr::Cast { expr, .. } => {
                Self::process_expr(expr.as_mut());
            }
            Expr::Nested(expr) => {
                Self::process_expr(expr.as_mut());
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                Self::process_expr(expr.as_mut());
            }
            Expr::InList { expr, list, .. } => {
                Self::process_expr(expr.as_mut());
                for item in list {
                    Self::process_expr(item);
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    Self::process_expr(op.as_mut());
                }
                for cw in conditions {
                    Self::process_expr(&mut cw.condition);
                    Self::process_expr(&mut cw.result);
                }
                if let Some(el) = else_result {
                    Self::process_expr(el.as_mut());
                }
            }
            Expr::InSubquery { expr, subquery, .. } => {
                Self::process_expr(expr.as_mut());
                Self::process_query(subquery);
            }
            Expr::Exists { subquery, .. } => {
                Self::process_query(subquery);
            }
            Expr::Subquery(subquery) => {
                Self::process_query(subquery);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::process_expr(expr.as_mut());
                Self::process_expr(low.as_mut());
                Self::process_expr(high.as_mut());
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::RLike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. } => {
                Self::process_expr(expr.as_mut());
                Self::process_expr(pattern.as_mut());
            }
            Expr::IsFalse(e)
            | Expr::IsNotFalse(e)
            | Expr::IsTrue(e)
            | Expr::IsNotTrue(e)
            | Expr::IsUnknown(e)
            | Expr::IsNotUnknown(e) => {
                Self::process_expr(e.as_mut());
            }
            Expr::IsDistinctFrom(l, r) | Expr::IsNotDistinctFrom(l, r) => {
                Self::process_expr(l.as_mut());
                Self::process_expr(r.as_mut());
            }
            Expr::Tuple(exprs) => {
                for e in exprs {
                    Self::process_expr(e);
                }
            }
            _ => {}
        }
    }

    fn process_window_type(window_type: &mut sqlparser::ast::WindowType) {
        match window_type {
            sqlparser::ast::WindowType::WindowSpec(spec) => {
                for expr in &mut spec.partition_by {
                    Self::process_expr(expr);
                }
                for ob in &mut spec.order_by {
                    Self::process_expr(&mut ob.expr);
                }
            }
            sqlparser::ast::WindowType::NamedWindow(ident) => {
                ident.quote_style = Some('"');
            }
        }
    }

    fn process_function_arguments(args: &mut sqlparser::ast::FunctionArguments) {
        match args {
            sqlparser::ast::FunctionArguments::List(list) => {
                for arg in &mut list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Named { arg, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                Self::process_expr(e);
                            }
                        }
                        sqlparser::ast::FunctionArg::Unnamed(arg_expr) => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg_expr {
                                Self::process_expr(e);
                            }
                        }
                        sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                            Self::process_expr(name);
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                Self::process_expr(e);
                            }
                        }
                    }
                }
            }
            sqlparser::ast::FunctionArguments::Subquery(query) => Self::process_query(query),
            sqlparser::ast::FunctionArguments::None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifiers_cte() {
        let sql = "WITH cte_a AS (SELECT id FROM raw_t) SELECT id FROM cte_a";
        let quoted = UniversalQuoter::quote_identifiers(sql).unwrap();
        assert!(quoted.contains("WITH \"cte_a\" AS (SELECT \"id\" FROM \"raw_t\")"));
        assert!(quoted.contains("SELECT \"id\" FROM \"cte_a\""));
    }

    #[test]
    fn test_quote_identifiers_builtin_func() {
        let sql = "SELECT COUNT(*), UPPER(name) FROM raw_t";
        let quoted = UniversalQuoter::quote_identifiers(sql).unwrap();
        // COUNT ne doit pas être cité
        assert!(quoted.contains("COUNT(*)"));
        assert!(quoted.contains("UPPER(\"name\")"));
    }

    #[test]
    fn test_quote_identifiers_union() {
        let sql = "SELECT a FROM t1 UNION SELECT a FROM t2";
        let quoted = UniversalQuoter::quote_identifiers(sql).unwrap();
        assert!(quoted.contains("SELECT \"a\" FROM \"t1\" UNION SELECT \"a\" FROM \"t2\""));
    }
}
