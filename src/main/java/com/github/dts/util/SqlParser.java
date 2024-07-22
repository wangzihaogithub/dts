package com.github.dts.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.github.dts.util.SchemaItem.ColumnItem;
import com.github.dts.util.SchemaItem.FieldItem;
import com.github.dts.util.SchemaItem.RelationFieldsPair;
import com.github.dts.util.SchemaItem.TableItem;

import java.util.*;
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

        Collection<ChangeSQL> changeSQLS = changeMergeSelect("        SELECT corpRelationTag.tag_id as tagId, corpTag.`name` as tagName, corpTag.category_id as categoryId, corpCategory.`name` as categoryName, corpCategory.sort as categorySort, corpCategory.`status` as categoryStatus, corpTag.source_enum as tagSource, corpTag.`status` as tagStatus, corpTag.change_flag as tagChangeFlag FROM corp_relation_tag corpRelationTag INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id  WHERE corpRelationTag.corp_id in (?,?)\n",

                Arrays.asList(new Object[]{1}, new Object[]{2}), false);

        Collection<ChangeSQL> changeSQLS2 = changeMergeSelect("        SELECT corpRelationTag.tag_id as tagId, corpTag.`name` as tagName, corpTag.category_id as categoryId, corpCategory.`name` as categoryName, corpCategory.sort as categorySort, corpCategory.`status` as categoryStatus, corpTag.source_enum as tagSource, corpTag.`status` as tagStatus, corpTag.change_flag as tagChangeFlag FROM corp_relation_tag corpRelationTag INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id  WHERE corpRelationTag.corp_id = ?\n",

                Arrays.asList(new Object[]{1}, new Object[]{2}), false);

        Collection<ChangeSQL> changeSQLS3 = changeMergeSelect("        SELECT corpRelationTag.tag_id as tagId, corpTag.`name` as tagName, corpTag.category_id as categoryId, corpCategory.`name` as categoryName, corpCategory.sort as categorySort, corpCategory.`status` as categoryStatus, corpTag.source_enum as tagSource, corpTag.`status` as tagStatus, corpTag.change_flag as tagChangeFlag FROM corp_relation_tag corpRelationTag INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id  WHERE corpRelationTag.corp_id = ? and corpRelationTag.id2 = 2\n",

                Arrays.asList(new Object[]{1}, new Object[]{2}), false);

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

            return schemaItem;
        } catch (Exception e) {
            if (e instanceof ParserException) {
                throw e;
            }
            throw new ParserException(e.toString(), e);
        }
    }

    public static Map<String, List<String>> getVarColumnList(String injectCondition) {
        if (injectCondition == null || injectCondition.isEmpty()) {
            return Collections.emptyMap();
        }
        injectCondition = injectCondition.trim();
        String injectConditionLower = injectCondition.toLowerCase();
        if (injectConditionLower.startsWith("where ")) {
            injectCondition = injectCondition.substring("where ".length());
        } else if (injectConditionLower.startsWith("and ")) {
            injectCondition = injectCondition.substring("and ".length());
        } else if (injectConditionLower.startsWith("or ")) {
            injectCondition = injectCondition.substring("or ".length());
        }

        SQLExpr injectConditionExpr = SQLUtils.toSQLExpr(injectCondition);
        Map<String, List<String>> map = new LinkedHashMap<>(2);
        injectConditionExpr.accept(new SQLASTVisitorAdapter() {

            @Override
            public boolean visit(SQLBinaryOpExpr x) {
                SQLBinaryOperator operator = x.getOperator();
                if (operator != SQLBinaryOperator.Equality) {
                    return super.visit(x);
                }
                SQLExpr left = x.getLeft();
                SQLExpr right = x.getRight();
                SQLExpr col = null;
                if (left instanceof SQLVariantRefExpr) {
                    col = right;
                } else if (right instanceof SQLVariantRefExpr) {
                    col = left;
                }
                if (col instanceof SQLPropertyExpr) {
                    map.computeIfAbsent(normalize(((SQLPropertyExpr) col).getOwnerName()), k -> new ArrayList<>())
                            .add(normalize(((SQLPropertyExpr) col).getName()));
                } else if (col instanceof SQLIdentifierExpr) {
                    map.computeIfAbsent("", k -> new ArrayList<>())
                            .add(normalize(((SQLIdentifierExpr) col).getName()));
                }
                return super.visit(x);
            }
        });
        return map;
    }

    public static Map<String, List<String>> getColumnList(String injectCondition) {
        if (injectCondition == null || injectCondition.isEmpty()) {
            return Collections.emptyMap();
        }
        injectCondition = injectCondition.trim();
        String injectConditionLower = injectCondition.toLowerCase();
        if (injectConditionLower.startsWith("where ")) {
            injectCondition = injectCondition.substring("where ".length());
        } else if (injectConditionLower.startsWith("and ")) {
            injectCondition = injectCondition.substring("and ".length());
        } else if (injectConditionLower.startsWith("or ")) {
            injectCondition = injectCondition.substring("or ".length());
        }
        if (injectCondition.isEmpty()) {
            return Collections.emptyMap();
        }
        SQLExpr injectConditionExpr = SQLUtils.toSQLExpr(injectCondition);
        Map<String, List<String>> map = new LinkedHashMap<>();
        injectConditionExpr.accept(new SQLASTVisitorAdapter() {

            @Override
            public boolean visit(SQLInSubQueryExpr statement) {
                SQLExpr expr = statement.getExpr();
                String owner;
                String name;
                if (expr instanceof SQLPropertyExpr) {
                    name = ((SQLPropertyExpr) expr).getName();
                    owner = Objects.toString(((SQLPropertyExpr) expr).getOwnerName(), "");
                } else if (expr instanceof SQLIdentifierExpr) {
                    name = ((SQLIdentifierExpr) expr).getName();
                    owner = "";
                } else {
                    return false;
                }
                String col = normalize(name);
                map.computeIfAbsent(owner, e -> new ArrayList<>()).add(col);
                return false;
            }

            @Override
            public boolean visit(SQLSelectQueryBlock statement) {
                return false;
            }

            @Override
            public boolean visit(SQLPropertyExpr x) {
                String col = normalize(x.getName());
                String owner = Objects.toString(x.getOwnerName(), "");
                map.computeIfAbsent(owner, e -> new ArrayList<>()).add(col);
                return true;
            }

            @Override
            public boolean visit(SQLIdentifierExpr x) {
//                String col = normalize(x.getName());
//
//                map.computeIfAbsent("",e-> new ArrayList<>()).add(col);
                return true;
            }
        });
        return map;
    }

    private static String normalize(String name) {
        return ESSyncUtil.stringCache(SQLUtils.normalize(name, null));
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
            fieldItem.setFieldName(cleanColumn(selectItem.getAlias()));
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
            String name = cleanColumn(identifierExpr.getName());
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(name);
                fieldItem.setExpr(identifierExpr.toString());
            }
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(name);
            fieldItem.addColumn(null, columnItem);
        } else if (expr instanceof SQLPropertyExpr) {
            // 有owner
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
            String name = cleanColumn(sqlPropertyExpr.getName());
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(name);
                fieldItem.setExpr(sqlPropertyExpr.toString());
            }
            String ownernName = cleanColumn(sqlPropertyExpr.getOwnernName());
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(name);
            columnItem.setOwner(ownernName);
            fieldItem.addColumn(ownernName, columnItem);
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
            tableItem.setTableName(cleanColumn(sqlExprTableSource.getName().getSimpleName()));
            if (tableItem.getAlias() == null) {
                tableItem.setAlias(sqlExprTableSource.getAlias());
            }
//            if (tableItems.isEmpty()) {
//                // 第一张表为主表
//                tableItem.setMain(true);
//            }
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
                throw new UnsupportedOperationException(expr + "Unsupported for complex of on-condition");
            }
            FieldItem rightFieldItem = new FieldItem();
            visitColumn(sqlBinaryOpExpr.getRight(), rightFieldItem);
            if (rightFieldItem.getColumnItems().size() != 1 || rightFieldItem.isMethod()
                    || rightFieldItem.isBinaryOp()) {
                throw new UnsupportedOperationException(expr + "Unsupported for complex of on-condition");
            }
            /*
             * 增加属性 -> 表用到的所有字段 (为了实现map复杂es对象的嵌套查询功能)
             * 2019年6月6日 13:37:41 王子豪
             */
            tableItem.getSchemaItem().getFields().put(leftFieldItem.getOwnerAndColumnName(), leftFieldItem);
            tableItem.getSchemaItem().getFields().put(rightFieldItem.getOwnerAndColumnName(), rightFieldItem);

            tableItem.getRelationFields().add(new RelationFieldsPair(leftFieldItem, rightFieldItem));
        } else {
            throw new UnsupportedOperationException(expr + "Unsupported for complex of on-condition");
        }
    }

    public static String changeSelect(String sql, Map<String, List<String>> columnList) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        selectList.clear();
        for (Map.Entry<String, List<String>> entry : columnList.entrySet()) {
            String owner = entry.getKey();
            LinkedHashSet<String> names = new LinkedHashSet<>(entry.getValue());
            for (String name : names) {
                selectList.add(new SQLSelectItem(new SQLPropertyExpr(owner, name)));
            }
        }
        return sqlStatement.toString();
    }

    public static List<ChangeSQL> changeMergeSelect(String sql, List<Object[]> args, boolean needGroupBy) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelect select = ((SQLSelectStatement) sqlStatement).getSelect();
            SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            SQLExpr where = queryBlock.getWhere();
            if (where instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr whereBinaryOp = ((SQLBinaryOpExpr) where);
                if (whereBinaryOp.getOperator() == SQLBinaryOperator.Equality) {
                    SQLExpr left = whereBinaryOp.getLeft();
                    SQLExpr right = whereBinaryOp.getRight();
                    if (left instanceof SQLVariantRefExpr && right instanceof SQLPropertyExpr) {
                        return mergeEqualitySql(args, sqlStatement, queryBlock,
                                (SQLVariantRefExpr) left, (SQLPropertyExpr) right, needGroupBy);
                    } else if (right instanceof SQLVariantRefExpr && left instanceof SQLPropertyExpr) {
                        return mergeEqualitySql(args, sqlStatement, queryBlock,
                                (SQLVariantRefExpr) right, (SQLPropertyExpr) left, needGroupBy);
                    }
                }
            }
        }
        return null;
    }

    private static List<ChangeSQL> mergeEqualitySql(List<Object[]> args,
                                                    SQLStatement sqlStatement,
                                                    SQLSelectQueryBlock queryBlock,
                                                    SQLVariantRefExpr right, SQLPropertyExpr leftExpr,
                                                    boolean needGroupBy) {
        String columnName = normalize(leftExpr.getName());
        List<String> addColumnNameList;
        SQLSelectItem selectItem = queryBlock.getSelectList().stream().filter(e -> equalsExpr(leftExpr, e.getExpr())).findFirst().orElse(null);
        String[] uniqueColumnNames;
        if (selectItem != null) {
            addColumnNameList = Collections.emptyList();
            String alias = selectItem.getAlias();
            uniqueColumnNames = new String[]{alias == null || alias.isEmpty() ? columnName : alias};
        } else {
            queryBlock.addSelectItem(leftExpr.clone());
            addColumnNameList = Collections.singletonList(columnName);
            uniqueColumnNames = new String[]{columnName};
        }
        if (needGroupBy && queryBlock.getGroupBy() == null) {
            SQLSelectGroupByClause groupByClause = new SQLSelectGroupByClause();
            groupByClause.addItem(leftExpr.clone());
            queryBlock.setGroupBy(groupByClause);
        }
        List<ChangeSQL> resultList = new ArrayList<>();
        SQLInListExpr inListExpr = new SQLInListExpr(leftExpr.clone());
        Object[] newArgs = new Object[args.size()];
        int i = 0;
        for (Object[] arg : args) {
            newArgs[i] = arg[0];
            inListExpr.addTarget(right.clone());
            i++;
        }
        queryBlock.setWhere(inListExpr);
        resultList.add(new ChangeSQL(ESSyncUtil.stringCacheLRU(sqlStatement.toString()), newArgs, uniqueColumnNames, addColumnNameList));
        return resultList;
    }

    private static boolean equalsExpr(SQLPropertyExpr expr, SQLExpr expr1) {
        if (expr1 instanceof SQLPropertyExpr) {
            String ownerName = normalize(expr.getOwnerName());
            String name = normalize(expr.getName());
            String ownerName1 = Objects.toString(normalize(((SQLPropertyExpr) expr1).getOwnerName()), "");
            String name1 = Objects.toString(normalize((((SQLPropertyExpr) expr1).getName())), "");
            return ownerName1.equalsIgnoreCase(ownerName) && name1.equalsIgnoreCase(name);
        } else if (expr1 instanceof SQLIdentifierExpr) {
            String name = normalize(expr.getName());
            String name1 = Objects.toString(normalize((((SQLIdentifierExpr) expr1).getName())), "");
            return name1.equalsIgnoreCase(name);
        } else {
            return false;
        }
    }

    public static class ChangeSQL {
        private final String sql;
        private final Object[] args;
        private final String[] uniqueColumnNames;
        private final List<String> addColumnNameList;

        public ChangeSQL(String sql, Object[] args, String[] uniqueColumnNames,
                         List<String> addColumnNameList) {
            this.sql = sql;
            this.args = args;
            this.uniqueColumnNames = uniqueColumnNames;
            this.addColumnNameList = addColumnNameList;
        }

        public String getSql() {
            return sql;
        }

        public Object[] getArgs() {
            return args;
        }

        public String[] getUniqueColumnNames() {
            return uniqueColumnNames;
        }

        public List<String> getAddColumnNameList() {
            return addColumnNameList;
        }
    }

    public static String cleanColumn(String column) {
        if (column == null) {
            return null;
        }
        if (column.contains("`")) {
            column = column.replaceAll("`", "");
        }

        if (column.contains("'")) {
            column = column.replaceAll("'", "");
        }

        if (column.contains("\"")) {
            column = column.replaceAll("\"", "");
        }

        return ESSyncUtil.stringCache(column);
    }
}
