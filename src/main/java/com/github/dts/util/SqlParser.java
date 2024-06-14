package com.github.dts.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.github.dts.util.SchemaItem.ColumnItem;
import com.github.dts.util.SchemaItem.FieldItem;
import com.github.dts.util.SchemaItem.RelationFieldsPair;
import com.github.dts.util.SchemaItem.TableItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ES同步指定sql格式解析
 *
 * @author rewerma 2018-10-26 下午03:45:49
 * @version 1.0.0
 */
public class SqlParser {

    public static void main(String[] args) {
        SchemaItem schemaItem =
                SqlParser.parse("select t1.id,t1.name,t1.pwd from user t1 " +
                        "left join order t2 on t2.user_id = t1.id " +
                        "where t1.name =#{name} and t1.id = 1 and t1.de_flag = 0");

        System.out.println(schemaItem);
    }

    /**
     * 解析sql
     *
     * @param sql sql
     * @return 视图对象
     */
    public static SchemaItem parse(String sql) {
        try {
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

            SchemaItem schemaItem = new SchemaItem();
            schemaItem.setSql(SQLUtils.toMySqlString(sqlSelectQueryBlock));
            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            List<TableItem> tableItems = new ArrayList<>();
            SqlParser.visitSelectTable(schemaItem, sqlTableSource, tableItems, null);
            tableItems.forEach(tableItem -> schemaItem.getAliasTableItems().put(tableItem.getAlias(), tableItem));

            List<FieldItem> selectFieldItems = collectSelectQueryFields(sqlSelectQueryBlock);
            selectFieldItems.forEach(fieldItem -> {
                schemaItem.getSelectFields().put(fieldItem.getFieldName(), fieldItem);
                schemaItem.getFields().put(fieldItem.getOwnerAndColumnName(), fieldItem);
            });

            List<FieldItem> whereFieldItems = new ArrayList<>();
            collectWhereFields(sqlSelectQueryBlock.getWhere(), null, whereFieldItems);
            whereFieldItems.forEach(fieldItem -> {
                schemaItem.getFields().put(fieldItem.getOwnerAndColumnName(), fieldItem);
            });

            schemaItem.init();

            if (schemaItem.getAliasTableItems().isEmpty() || schemaItem.getSelectFields().isEmpty()) {
                throw new ParserException("Parse sql error");
            }
            return schemaItem;
        } catch (Exception e) {
            if (e instanceof ParserException) {
                throw e;
            }
            throw new ParserException(e.getMessage(), e);
        }
    }


    /**
     * 归集字段 (where 条件中的)
     *
     * @param left
     * @param right
     * @param fieldItems
     */
    private static void collectWhereFields(SQLExpr left, SQLExpr right, List<FieldItem> fieldItems) {
        if (left instanceof SQLBinaryOpExpr) {
            collectWhereFields(((SQLBinaryOpExpr) left).getLeft(), ((SQLBinaryOpExpr) left).getRight(), fieldItems);
        }
        if (right instanceof SQLBinaryOpExpr) {
            collectWhereFields(((SQLBinaryOpExpr) right).getLeft(), ((SQLBinaryOpExpr) right).getRight(), fieldItems);
        }
        FieldItem leftFieldItem = new FieldItem();
        FieldItem rightFieldItem = new FieldItem();
        visitColumn(left, leftFieldItem);
        visitColumn(right, rightFieldItem);
        if (leftFieldItem.getFieldName() != null) {
            fieldItems.add(leftFieldItem);
        }
        if (rightFieldItem.getFieldName() != null) {
            fieldItems.add(rightFieldItem);
        }
    }

    /**
     * 归集字段
     *
     * @param sqlSelectQueryBlock sqlSelectQueryBlock
     * @return 字段属性列表
     */
    private static List<FieldItem> collectSelectQueryFields(MySqlSelectQueryBlock sqlSelectQueryBlock) {
        return sqlSelectQueryBlock.getSelectList().stream().map(selectItem -> {
            FieldItem fieldItem = new FieldItem();
            fieldItem.setFieldName(Util.cleanColumn(selectItem.getAlias()));
            fieldItem.setExpr(selectItem.toString());
            visitColumn(selectItem.getExpr(), fieldItem);
            return fieldItem;
        }).collect(Collectors.toList());
    }

    /**
     * 解析字段
     *
     * @param expr      sql expr
     * @param fieldItem 字段属性
     */
    private static void visitColumn(SQLExpr expr, FieldItem fieldItem) {
        if (expr instanceof SQLIdentifierExpr) {
            // 无owner
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            String name = Util.cleanColumn(identifierExpr.getName());
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(name);
                fieldItem.setExpr(identifierExpr.toString());
            }
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(name);
            fieldItem.getOwners().add(null);
            fieldItem.addColumn(columnItem);
        } else if (expr instanceof SQLPropertyExpr) {
            // 有owner
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
            String name = Util.cleanColumn(sqlPropertyExpr.getName());
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(name);
                fieldItem.setExpr(sqlPropertyExpr.toString());
            }
            String ownernName = Util.cleanColumn(sqlPropertyExpr.getOwnernName());
            fieldItem.getOwners().add(ownernName);
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(name);
            columnItem.setOwner(ownernName);
            fieldItem.addColumn(columnItem);
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
            fieldItem.setMethod(true);
            for (SQLExpr sqlExpr : methodInvokeExpr.getArguments()) {
                visitColumn(sqlExpr, fieldItem);
            }
        } else if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            fieldItem.setBinaryOp(true);
            visitColumn(sqlBinaryOpExpr.getLeft(), fieldItem);
            visitColumn(sqlBinaryOpExpr.getRight(), fieldItem);
        } else if (expr instanceof SQLCaseExpr) {
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) expr;
            fieldItem.setMethod(true);
            sqlCaseExpr.getItems().forEach(item -> visitColumn(item.getConditionExpr(), fieldItem));
        } else {
//            LOGGER.warn("skip filed. expr={}",expr);
        }
    }

    /**
     * 解析表
     *
     * @param schemaItem     视图对象
     * @param sqlTableSource sqlTableSource
     * @param tableItems     表对象列表
     * @param tableItemTmp   表对象(临时)
     */
    private static void visitSelectTable(SchemaItem schemaItem, SQLTableSource sqlTableSource,
                                         List<TableItem> tableItems, TableItem tableItemTmp) {
        if (sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem(schemaItem);
            }
            tableItem.setSchema(sqlExprTableSource.getSchema());
            tableItem.setTableName(Util.cleanColumn(sqlExprTableSource.getName().getSimpleName()));
            if (tableItem.getAlias() == null) {
                tableItem.setAlias(sqlExprTableSource.getAlias());
            }
            if (tableItems.isEmpty()) {
                // 第一张表为主表
                tableItem.setMain(true);
            }
            tableItems.add(tableItem);
        } else if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource leftTableSource = sqlJoinTableSource.getLeft();
            visitSelectTable(schemaItem, leftTableSource, tableItems, null);
            SQLTableSource rightTableSource = sqlJoinTableSource.getRight();
            TableItem rightTableItem = new TableItem(schemaItem);
            // 解析on条件字段
            visitOnCondition(sqlJoinTableSource.getCondition(), rightTableItem);
            visitSelectTable(schemaItem, rightTableSource, tableItems, rightTableItem);

        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            MySqlSelectQueryBlock sqlSelectQuery = (MySqlSelectQueryBlock) subQueryTableSource.getSelect().getQuery();
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem(schemaItem);
            }
            tableItem.setAlias(subQueryTableSource.getAlias());
            tableItem.setSubQuerySql(SQLUtils.toMySqlString(sqlSelectQuery));
            tableItem.setSubQuery(true);
            tableItem.setSubQueryFields(collectSelectQueryFields(sqlSelectQuery));
            visitSelectTable(schemaItem, sqlSelectQuery.getFrom(), tableItems, tableItem);
        }
    }

    /**
     * 解析on条件
     *
     * @param expr      sql expr
     * @param tableItem 表对象
     */
    private static void visitOnCondition(SQLExpr expr, TableItem tableItem) {
        if (!(expr instanceof SQLBinaryOpExpr)) {
            throw new UnsupportedOperationException();
        }
        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
        if (sqlBinaryOpExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
            visitOnCondition(sqlBinaryOpExpr.getLeft(), tableItem);
            visitOnCondition(sqlBinaryOpExpr.getRight(), tableItem);
        } else if (sqlBinaryOpExpr.getOperator() == SQLBinaryOperator.Equality) {
            FieldItem leftFieldItem = new FieldItem();
            visitColumn(sqlBinaryOpExpr.getLeft(), leftFieldItem);
            if (leftFieldItem.getColumnItems().size() != 1 || leftFieldItem.isMethod() || leftFieldItem.isBinaryOp()) {
                throw new UnsupportedOperationException("Unsupported for complex of on-condition");
            }
            FieldItem rightFieldItem = new FieldItem();
            visitColumn(sqlBinaryOpExpr.getRight(), rightFieldItem);
            if (rightFieldItem.getColumnItems().size() != 1 || rightFieldItem.isMethod()
                    || rightFieldItem.isBinaryOp()) {
                throw new UnsupportedOperationException("Unsupported for complex of on-condition");
            }
            /*
             * 增加属性 -> 表用到的所有字段 (为了实现map复杂es对象的嵌套查询功能)
             * 2019年6月6日 13:37:41 王子豪
             */
            tableItem.getSchemaItem().getFields().put(leftFieldItem.getOwnerAndColumnName(), leftFieldItem);
            tableItem.getSchemaItem().getFields().put(rightFieldItem.getOwnerAndColumnName(), rightFieldItem);

            tableItem.getRelationFields().add(new RelationFieldsPair(leftFieldItem, rightFieldItem));
        } else {
            throw new UnsupportedOperationException("Unsupported for complex of on-condition");
        }
    }
}
