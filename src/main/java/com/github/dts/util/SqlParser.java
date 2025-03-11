package com.github.dts.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.VisitorFeature;
import com.github.dts.util.SchemaItem.FieldItem;
import com.github.dts.util.SchemaItem.RelationFieldsPair;
import com.github.dts.util.SchemaItem.TableItem;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ES同步指定sql格式解析
 *
 * @author rewerma 2018-10-26 下午03:45:49
 * @version 1.0.0
 */
public class SqlParser {
    public static final Map<String, String> CHANGE_SELECT_CACHE = new ConcurrentHashMap<>();
    private static final SQLExpr TRUE_EXPR = SQLUtils.toMySqlExpr("true");
    private static final Map<String, Map<String, List<String>>> GET_COLUMN_LIST_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, List<String>>> ORDER_BY_COLUMN_LIST_CACHE = new ConcurrentHashMap<>();

    private static final Map<String, String> REMOVE_GROUP_BY_CACHE = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(
                "select DISTINCT(job_id) from j"
        );
        SQLStatement sqlStatement1 = SQLUtils.parseSingleMysqlStatement(
                "select job_id from j"
        );
        SchemaItem schemaItem =
                SqlParser.parse("select t1.id,t1.name,t1.pwd from user t1 " +
                        "left join order t2 on t2.user_id = t1.id " +
                        "where t1.name =#{name} and t1.id = 1 and t1.de_flag = 0", true);

//        Collection<ChangeSQL> changeSQLS = changeMergeSelect("        SELECT corpRelationTag.tag_id as tagId, corpTag.`name` as tagName, corpTag.category_id as categoryId, corpCategory.`name` as categoryName, corpCategory.sort as categorySort, corpCategory.`status` as categoryStatus, corpTag.source_enum as tagSource, corpTag.`status` as tagStatus, corpTag.change_flag as tagChangeFlag FROM corp_relation_tag corpRelationTag INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id  WHERE corpRelationTag.corp_id in (?,?)\n",
//
//                Arrays.asList(new Object[]{1}, new Object[]{2}), null);

        ChangeSQL changeSQLS2 = changeMergeSelect("        SELECT corpRelationTag.tag_id as tagId, corpTag.`name` as tagName, corpTag.category_id as categoryId, corpCategory.`name` as categoryName, corpCategory.sort as categorySort, corpCategory.`status` as categoryStatus, corpTag.source_enum as tagSource, " +
                        "corpTag.`status` as tagStatus, corpTag.change_flag as tagChangeFlag FROM corp_relation_tag corpRelationTag INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id " +
                        " WHERE (tagChangeFlag =1 or tagName =2 ) and (tagChangeFlag =1 or tagName =2 and  (tagChangeFlag =1 or tagName =2 )) and (tagChangeFlag =1 or tagName =2 ) and tagStatus= 2 and tagStatus= ? and tagId =1\n",

                Arrays.asList(new Object[]{1}, new Object[]{2}), Stream.of("corpRelationTag.id ", "corpRelationTag.id2").map(ColumnItem::parse).collect(Collectors.toList()));

        ChangeSQL changeSQLS3 = changeMergeSelect("        SELECT corpRelationTag.tag_id as tagId, corpTag.`name` as tagName, corpTag.category_id as categoryId, corpCategory.`name` as categoryName, corpCategory.sort as categorySort, corpCategory.`status` as categoryStatus, corpTag.source_enum as tagSource, corpTag.`status` as tagStatus, corpTag.change_flag as tagChangeFlag FROM corp_relation_tag corpRelationTag INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id  WHERE corpRelationTag.corp_id = ? and corpRelationTag.id2 = 2\n",

                Arrays.asList(new Object[]{1}, new Object[]{2}), null);

        System.out.println(schemaItem);
    }

    /**
     * 解析sql
     *
     * @param sql   sql
     * @param check check是否校验
     * @return 视图对象
     */
    public static SchemaItem parse(String sql, boolean check) {
        try {
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

            SchemaItem schemaItem = new SchemaItem(check);
            schemaItem.setSql(toMysqlString(sqlSelectQueryBlock));
            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            List<TableItem> tableItems = new ArrayList<>();
            SqlParser.visitSelectTable(schemaItem, sqlTableSource, tableItems, null, check);
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

    public static String changePage(String sql, Integer pageNo, Integer pageSize) {
        if (pageSize == null) {
            throw new IllegalArgumentException("pageSize must not be null:" + sql);
        }
        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        if (statement instanceof SQLSelectStatement) {
            SQLSelectStatement select = (SQLSelectStatement) statement;
            SQLLimit limit = new SQLLimit();
            if (pageNo != null) {
                limit.setOffset(Math.max(0, (pageNo - 1) * pageSize));
            }
            limit.setRowCount(pageSize);
            select.getSelect().getQueryBlock().setLimit(limit);
            return toMysqlString(statement);
        } else {
            return sql;
        }
    }

    public static String trimSchema(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        try {
            SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
            statement.accept(new SQLASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    x.setSchema(null);
                    return true;
                }
            });
            return toMysqlString(statement);
        } catch (Exception e) {
            return sql;
        }
    }

    private static SQLExpr getInjectWhere(String injectCondition) {
        SQLSelectStatement statement = parseInjectStatement(injectCondition);
        return statement.getSelect().getQueryBlock().getWhere();
    }

    private static SQLOrderBy getInjectOrderBy(String injectCondition) {
        SQLSelectStatement statement = parseInjectStatement(injectCondition);
        return statement.getSelect().getQueryBlock().getOrderBy();
    }

    public static boolean existSelectLimit(String selectSql) {
        SQLSelectStatement statement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(selectSql);
        return statement.getSelect().getQueryBlock().getLimit() != null;
    }

    public static boolean existInjectGroupBy(String injectCondition) {
        SQLSelectStatement statement = parseInjectStatement(injectCondition);
        return statement.getSelect().getQueryBlock().getGroupBy() != null;
    }

    public static boolean existInjectLimit(String injectCondition) {
        SQLSelectStatement statement = parseInjectStatement(injectCondition);
        return statement.getSelect().getQueryBlock().getLimit() != null;
    }

    private static SQLSelectStatement parseInjectStatement(String injectCondition) {
        return (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement("select 1 from dual where " + trimInjectCondition(injectCondition));
    }

    public static List<BinaryOpExpr> getVarColumnList(String injectCondition) {
        if (injectCondition == null || injectCondition.isEmpty()) {
            return Collections.emptyList();
        }

        SQLExpr injectConditionExpr = getInjectWhere(injectCondition);
        List<BinaryOpExpr> list = new ArrayList<>();
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
                SQLVariantRefExpr val = null;
                if (left instanceof SQLVariantRefExpr) {
                    col = right;
                    val = (SQLVariantRefExpr) left;
                } else if (right instanceof SQLVariantRefExpr) {
                    col = left;
                    val = (SQLVariantRefExpr) right;
                }
                if (col instanceof SQLPropertyExpr) {
                    list.add(new BinaryOpExpr(normalize(((SQLPropertyExpr) col).getOwnerName()),
                            normalize(((SQLPropertyExpr) col).getName()),
                            normalize(val.getName())
                    ));
                } else if (col instanceof SQLIdentifierExpr) {
                    list.add(new BinaryOpExpr("",
                            normalize(((SQLIdentifierExpr) col).getName()),
                            normalize(val.getName())
                    ));
                }
                return super.visit(x);
            }
        });
        return list;
    }

    public static Map<String, List<String>> getColumnList(String injectConditionReq) {
        if (injectConditionReq == null || injectConditionReq.isEmpty()) {
            return Collections.emptyMap();
        }
        return GET_COLUMN_LIST_CACHE.computeIfAbsent(injectConditionReq, injectCondition -> {
            SQLExpr injectConditionExpr = getInjectWhere(injectCondition);
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
        });
    }

    /**
     * 表的别名和字段的关系 《表的别名，List《字段》》，如果别名为空表示用户没写别名，默认为主表
     *
     * @param injectConditionReq sql条件
     * @return 表的别名和字段的关系 《表的别名，List《字段》》，如果别名为空表示用户没写别名，默认为主表
     */
    public static Map<String, List<String>> getOrderByColumnList(String injectConditionReq) {
        if (injectConditionReq == null || injectConditionReq.isEmpty()) {
            return Collections.emptyMap();
        }
        return ORDER_BY_COLUMN_LIST_CACHE.computeIfAbsent(injectConditionReq, injectCondition -> {
            SQLOrderBy injectConditionExpr = getInjectOrderBy(injectCondition);
            Map<String, List<String>> map = new LinkedHashMap<>(2);
            if (injectConditionExpr == null) {
                return map;
            }
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
        });
    }

    public static String setGroupBy(String sql, Collection<ColumnItem> needGroupBy) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelect select = ((SQLSelectStatement) sqlStatement).getSelect();
            SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
            if (groupBy == null) {
                SQLSelectGroupByClause groupByClause = getSqlSelectGroupByClause(needGroupBy);
                queryBlock.setGroupBy(groupByClause);
                return toMysqlString(sqlStatement);
            }
        }
        return sql;
    }

    private static String toMysqlString(SQLObject sqlObject) {
        return SQLUtils.toMySqlString(sqlObject, new VisitorFeature[0]);
    }

    private static String normalize(String name) {
        return ESSyncUtil.stringCache(SQLUtils.normalize(name, null));
    }

    private static String trimInjectCondition(String injectCondition) {
        injectCondition = injectCondition.trim();
        String injectConditionLower = injectCondition.toLowerCase();
        if (injectConditionLower.startsWith("where ")) {
            injectCondition = injectCondition.substring("where ".length());
        } else if (injectConditionLower.startsWith("and ")) {
            injectCondition = injectCondition.substring("and ".length());
        } else if (injectConditionLower.startsWith("or ")) {
            injectCondition = injectCondition.substring("or ".length());
        }
        return injectCondition;
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
            if (fieldItem.getFieldName() == null) {
                throw new IllegalArgumentException("sql parse error! columnName is null. Please check your SQL statement. error sql = " + sqlSelectQueryBlock);
            }
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
     * @param check          是否校验
     */
    private static void visitSelectTable(SchemaItem schemaItem, SQLTableSource sqlTableSource,
                                         List<TableItem> tableItems, TableItem tableItemTmp, boolean check) {
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
            visitSelectTable(schemaItem, leftTableSource, tableItems, null, check);
            SQLTableSource rightTableSource = sqlJoinTableSource.getRight();
            TableItem rightTableItem = new TableItem(schemaItem);
            // 解析on条件字段
            visitOnCondition(sqlJoinTableSource.getCondition(), rightTableItem, check);
            visitSelectTable(schemaItem, rightTableSource, tableItems, rightTableItem, check);

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
            visitSelectTable(schemaItem, sqlSelectQuery.getFrom(), tableItems, tableItem, check);
        }
    }

    /**
     * 解析on条件
     *
     * @param expr      sql expr
     * @param tableItem 表对象
     */
    private static void visitOnCondition(SQLExpr expr, TableItem tableItem, boolean check) {
        if (!(expr instanceof SQLBinaryOpExpr)) {
            throw new UnsupportedOperationException();
        }
        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
        if (sqlBinaryOpExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
            visitOnCondition(sqlBinaryOpExpr.getLeft(), tableItem, check);
            visitOnCondition(sqlBinaryOpExpr.getRight(), tableItem, check);
        } else if (sqlBinaryOpExpr.getOperator() == SQLBinaryOperator.Equality) {
            FieldItem leftFieldItem = new FieldItem();
            SQLExpr left = sqlBinaryOpExpr.getLeft();
            visitColumn(left, leftFieldItem);
            Object leftValue = null;
            if (leftFieldItem.getColumnItems().size() != 1 || leftFieldItem.isMethod() || leftFieldItem.isBinaryOp()) {
                if (!check && left instanceof SQLValuableExpr) {
                    leftValue = ((SQLValuableExpr) left).getValue();
                } else {
                    throw new UnsupportedOperationException(expr + "Unsupported for complex of on-condition");
                }
            }
            FieldItem rightFieldItem = new FieldItem();
            SQLExpr right = sqlBinaryOpExpr.getRight();
            visitColumn(right, rightFieldItem);
            Object rightValue = null;
            if (rightFieldItem.getColumnItems().size() != 1 || rightFieldItem.isMethod()
                    || rightFieldItem.isBinaryOp()) {
                if (!check && right instanceof SQLValuableExpr) {
                    rightValue = ((SQLValuableExpr) right).getValue();
                } else {
                    throw new UnsupportedOperationException(expr + "Unsupported for complex of on-condition");
                }
            }
            /*
             * 增加属性 -> 表用到的所有字段 (为了实现map复杂es对象的嵌套查询功能)
             * 2019年6月6日 13:37:41 王子豪
             */
            if (!leftFieldItem.getColumnItems().isEmpty()) {
                tableItem.getSchemaItem().getFields().put(leftFieldItem.getOwnerAndColumnName(), leftFieldItem);
            }
            if (!rightFieldItem.getColumnItems().isEmpty()) {
                tableItem.getSchemaItem().getFields().put(rightFieldItem.getOwnerAndColumnName(), rightFieldItem);
            }
            tableItem.getRelationFields().add(new RelationFieldsPair(leftFieldItem, leftValue, rightFieldItem, rightValue));
        } else {
            throw new UnsupportedOperationException(expr + "Unsupported for complex of on-condition");
        }
    }

    public static String changeSelect(String sql, Map<String, List<String>> columnList, boolean distinct) {
        return CHANGE_SELECT_CACHE.computeIfAbsent(sql + columnList + distinct, unused -> {
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

            SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
            if (distinct) {
                queryBlock.setDistinct();
            }
            List<SQLSelectItem> selectList = queryBlock.getSelectList();
            selectList.clear();
            for (Map.Entry<String, List<String>> entry : columnList.entrySet()) {
                String owner = entry.getKey();
                LinkedHashSet<String> names = new LinkedHashSet<>(entry.getValue());
                for (String name : names) {
                    selectList.add(new SQLSelectItem(new SQLPropertyExpr(owner, name)));
                }
            }
            return toMysqlString(sqlStatement);
        });
    }

    public static ChangeSQL changeMergeSelect(String sql, Collection<Object[]> args, Collection<ColumnItem> needGroupBy) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelect select = ((SQLSelectStatement) sqlStatement).getSelect();
            SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            SQLExpr where = queryBlock.getWhere();
            if (where instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr whereBinaryOp = ((SQLBinaryOpExpr) where);
                SQLBinaryOperator andOp = SQLBinaryOperator.BooleanAnd;
                VarExprCollectResult varExprCollectResult = parseVarExpr(whereBinaryOp, andOp);
                if (varExprCollectResult != null && varExprCollectResult.varExpr.size() == 1) {
                    return changeMergeSelect(args, needGroupBy, sqlStatement, queryBlock,
                            varExprCollectResult.varExpr.get(0), varExprCollectResult.andExpr, andOp);
                }
            }
        }
        return null;
    }

    private static VarExprCollectResult parseVarExpr(SQLBinaryOpExpr where, SQLBinaryOperator operator) {
        VarExprCollectResult result = new VarExprCollectResult();
        if (isVarExpr(where)) {
            result.andExpr = null;
            result.varExpr = Collections.singletonList(where.clone());
            return result;
        } else if (where.getOperator() == operator) {
            result.andExpr = where.clone();
            result.varExpr = new ArrayList<>(1);
            List<SQLBinaryOpExpr> temp = new ArrayList<>(3);
            temp.add(result.andExpr);
            while (!temp.isEmpty()) {
                SQLBinaryOpExpr expr = temp.remove(0);
                SQLExpr left = expr.getLeft();
                SQLExpr right = expr.getRight();
                if (left instanceof SQLBinaryOpExpr) {
                    if (isVarExpr((SQLBinaryOpExpr) left)) {
                        result.varExpr.add((SQLBinaryOpExpr) left);
                        expr.setLeft(TRUE_EXPR.clone());
                    } else {
                        temp.add((SQLBinaryOpExpr) left);
                    }
                }
                if (right instanceof SQLBinaryOpExpr) {
                    if (isVarExpr((SQLBinaryOpExpr) right)) {
                        result.varExpr.add((SQLBinaryOpExpr) right);
                        expr.setRight(TRUE_EXPR.clone());
                    } else {
                        temp.add((SQLBinaryOpExpr) right);
                    }
                }
            }
            return result;
        } else {
            return null;
        }
    }

    private static boolean isVarExpr(SQLBinaryOpExpr expr) {
        if (expr.getOperator() == SQLBinaryOperator.Equality) {
            SQLExpr left1 = expr.getLeft();
            SQLExpr right1 = expr.getRight();
            if (left1 instanceof SQLVariantRefExpr && right1 instanceof SQLPropertyExpr) {
                return true;
            } else if (right1 instanceof SQLVariantRefExpr && left1 instanceof SQLPropertyExpr) {
                return true;
            } else if (left1 instanceof SQLVariantRefExpr && right1 instanceof SQLIdentifierExpr) {
                return true;
            } else if (right1 instanceof SQLVariantRefExpr && left1 instanceof SQLIdentifierExpr) {
                return true;
            }
        }
        return false;
    }

    private static ChangeSQL changeMergeSelect(Collection<Object[]> args, Collection<ColumnItem> needGroupBy,
                                               SQLStatement sqlStatement, SQLSelectQueryBlock queryBlock,
                                               SQLBinaryOpExpr whereBinaryOp, SQLBinaryOpExpr rightExpr,
                                               SQLBinaryOperator rightOperator) {
        SQLExpr left = whereBinaryOp.getLeft();
        SQLExpr right = whereBinaryOp.getRight();
        if (left instanceof SQLIdentifierExpr) {
            left = ref(queryBlock.getSelectList(), (SQLIdentifierExpr) left);
        } else if (right instanceof SQLIdentifierExpr) {
            right = ref(queryBlock.getSelectList(), (SQLIdentifierExpr) right);
        }
        if (left instanceof SQLVariantRefExpr && right instanceof SQLPropertyExpr) {
            return mergeEqualitySql(args, sqlStatement, queryBlock,
                    (SQLVariantRefExpr) left, right, ((SQLPropertyExpr) right).getName(), needGroupBy, rightExpr, rightOperator);
        } else if (right instanceof SQLVariantRefExpr && left instanceof SQLPropertyExpr) {
            return mergeEqualitySql(args, sqlStatement, queryBlock,
                    (SQLVariantRefExpr) right, left, ((SQLPropertyExpr) left).getName(), needGroupBy, rightExpr, rightOperator);
        } else if (left instanceof SQLVariantRefExpr && right instanceof SQLIdentifierExpr) {
            return mergeEqualitySql(args, sqlStatement, queryBlock,
                    (SQLVariantRefExpr) left, right, ((SQLIdentifierExpr) right).getName(), needGroupBy, rightExpr, rightOperator);
        } else if (right instanceof SQLVariantRefExpr && left instanceof SQLIdentifierExpr) {
            return mergeEqualitySql(args, sqlStatement, queryBlock,
                    (SQLVariantRefExpr) right, left, ((SQLIdentifierExpr) left).getName(), needGroupBy, rightExpr, rightOperator);
        } else {
            return null;
        }
    }

    private static SQLExpr ref(List<SQLSelectItem> selectItemList, SQLIdentifierExpr leftExpr) {
        String name = cleanColumn(leftExpr.getName());
        SQLSelectItem selectItem = selectItemList.stream().filter(e -> equalsExpr(name, e)).findFirst().orElse(null);
        if (selectItem != null) {
            return selectItem.getExpr();
        } else {
            return leftExpr;
        }
    }

    private static ChangeSQL mergeEqualitySql(Collection<Object[]> args,
                                              SQLStatement sqlStatement,
                                              SQLSelectQueryBlock queryBlock,
                                              SQLVariantRefExpr right, SQLExpr leftExpr,
                                              String leftExprName,
                                              Collection<ColumnItem> needGroupBy,
                                              SQLBinaryOpExpr andExpr,
                                              SQLBinaryOperator operator) {
        String columnName = normalize(leftExprName);

        if (needGroupBy != null && !needGroupBy.isEmpty() && queryBlock.getGroupBy() == null) {
            queryBlock.setGroupBy(getSqlSelectGroupByClause(needGroupBy));
        }

        SQLSelectItem selectItem = queryBlock.getSelectList().stream().filter(e -> equalsExpr(columnName, e)).findFirst().orElse(null);
        String[] uniqueColumnNames;
        List<String> addColumnNameList;
        if (selectItem != null) {
            addColumnNameList = Collections.emptyList();
            String alias = selectItem.getAlias();
            uniqueColumnNames = new String[]{alias == null || alias.isEmpty() ? columnName : alias};
        } else {
            queryBlock.addSelectItem(leftExpr.clone());
            addColumnNameList = Collections.singletonList(columnName);
            uniqueColumnNames = new String[]{columnName};
        }
        SQLInListExpr inListExpr = new SQLInListExpr(leftExpr.clone());
        Object[] newArgs = new Object[args.size()];
        int i = 0;
        for (Object[] arg : args) {
            newArgs[i] = arg[0];
            inListExpr.addTarget(right.clone());
            i++;
        }
        if (andExpr == null) {
            queryBlock.setWhere(inListExpr);
        } else {
            queryBlock.setWhere(new SQLBinaryOpExpr(inListExpr, operator, andExpr));
        }
        return new ChangeSQL(ESSyncUtil.stringCacheLRU(toMysqlString(sqlStatement)), newArgs, uniqueColumnNames, addColumnNameList);
    }

    private static SQLSelectGroupByClause getSqlSelectGroupByClause(Collection<ColumnItem> needGroupBy) {
        SQLSelectGroupByClause groupByClause = new SQLSelectGroupByClause();
        for (ColumnItem columnItem : needGroupBy) {
            String owner = columnItem.getOwner();
            String groupByColumnName = columnItem.getColumnName();
            SQLExpr expr;
            if (owner == null || owner.isEmpty()) {
                expr = new SQLIdentifierExpr(groupByColumnName);
            } else {
                expr = new SQLPropertyExpr(owner, groupByColumnName);
            }
            groupByClause.addItem(expr);
        }
        return groupByClause;
    }

    private static String name(SQLExpr expr) {
        if (expr instanceof SQLPropertyExpr) {
            return cleanColumn(((SQLPropertyExpr) expr).getName());
        } else if (expr instanceof SQLIdentifierExpr) {
            return cleanColumn(((SQLIdentifierExpr) expr).getName());
        } else {
            return null;
        }
    }

    private static boolean equalsExpr(String name, SQLSelectItem item) {
        String alias = item.getAlias();
        if (alias != null) {
            return alias.equals(name);
        } else {
            SQLExpr expr = item.getExpr();
            if (expr instanceof SQLPropertyExpr) {
                return name.equalsIgnoreCase(cleanColumn(((SQLPropertyExpr) expr).getName()));
            } else if (expr instanceof SQLIdentifierExpr) {
                return name.equalsIgnoreCase(cleanColumn(((SQLIdentifierExpr) expr).getName()));
            } else {
                return false;
            }
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

    public static String[] getGroupByIdColumns(String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        List<String> list = new ArrayList<>();
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelect select = ((SQLSelectStatement) sqlStatement).getSelect();
            SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
            if (groupBy != null) {
                List<SQLExpr> temp = new ArrayList<>(groupBy.getItems());
                while (!temp.isEmpty()) {
                    SQLExpr item = temp.remove(0);
                    if (item instanceof SQLIdentifierExpr) {
                        String name = cleanColumn(((SQLIdentifierExpr) item).getName());
                        SQLSelectItem selectItem = queryBlock.getSelectList().stream().filter(e -> equalsExpr(name, e)).findFirst().orElse(null);
                        if (selectItem != null) {
                            temp.add(selectItem.getExpr());
                        } else {
                            list.add(name);
                        }
                    } else if (item instanceof SQLPropertyExpr) {
                        String ownerName = cleanColumn(((SQLPropertyExpr) item).getOwnerName());
                        String name = cleanColumn(((SQLPropertyExpr) item).getName());
                        list.add(ownerName + "." + name);
                    } else if (item instanceof SQLMethodInvokeExpr) {
                        temp.addAll(((SQLMethodInvokeExpr) item).getArguments());
                    }
                }
            }
        }
        return list.toArray(new String[0]);
    }

    public static String removeGroupBy(String sql) {
        return REMOVE_GROUP_BY_CACHE.computeIfAbsent(sql, key -> {
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(key);
            if (sqlStatement instanceof SQLSelectStatement) {
                SQLSelect select = ((SQLSelectStatement) sqlStatement).getSelect();
                SQLSelectQueryBlock queryBlock = select.getQueryBlock();
                SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
                if (groupBy != null) {
                    queryBlock.setGroupBy(null);
                }
            }
            return toMysqlString(sqlStatement);
        });
    }

    static class VarExprCollectResult {
        List<SQLBinaryOpExpr> varExpr;
        SQLBinaryOpExpr andExpr;
    }

    public static class BinaryOpExpr {
        private final String owner;
        private final String name;
        private final String value;

        public BinaryOpExpr(String owner, String name, String value) {
            this.owner = owner;
            this.name = name;
            this.value = value;
        }

        @Override
        public String toString() {
            return Objects.toString(owner, "") + "." + name + "=" + value;
        }

        public boolean isOwner(String owner) {
            return Objects.equals(owner, this.owner);
        }

        public String getOwner() {
            return owner;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
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
}
