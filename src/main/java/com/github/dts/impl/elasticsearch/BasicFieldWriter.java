package com.github.dts.impl.elasticsearch;

import com.github.dts.impl.elasticsearch.basic.*;
import com.github.dts.impl.elasticsearch.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.FieldItem;
import com.github.dts.util.SchemaItem.TableItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * ES 同步 Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class BasicFieldWriter {

    private static final Logger logger = LoggerFactory.getLogger(BasicFieldWriter.class);
    private final ESTemplate esTemplate;

    public BasicFieldWriter(ESTemplate esTemplate) {
        this.esTemplate = esTemplate;
    }

    public static void executeUpdate(List<ESSyncConfigSQL> sqlList, int maxIdInCount) {
        List<MergeJdbcTemplateSQL<ESSyncConfigSQL>> mergeList = MergeJdbcTemplateSQL.merge(sqlList, maxIdInCount);
        MergeJdbcTemplateSQL.executeQueryList(mergeList, null, ESSyncConfigSQL::run);
    }

    private static boolean isMainTable(SchemaItem schemaItem, String table) {
        return schemaItem.getMainTable().getTableName().equalsIgnoreCase(table);
    }

    private static Object putAllValueAndMysql2EsType(ESMapping mapping, Map<String, Object> dmlData,
                                                     Map<String, Object> esFieldData, ESTemplate esTemplate) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = EsGetterUtil.invokeGetterValue(mapping, dmlData);
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getColumnItems().isEmpty()) {
                continue;
            }
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();

            Object value = getColumnValueAndMysql2EsType(mapping, dmlData, fieldName, columnName, esTemplate);

            if (!mapping.isWriteNull() && value == null) {
                continue;
            }
            esFieldData.put(fieldName, value);
        }

        return resultIdVal;
    }

    private static Object putAllValueAndMysql2EsType(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                                     Map<String, Object> esFieldData, String tableName, ESTemplate esTemplate) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        List<String> aliases = schemaItem.getTableItemAliases(tableName);

        Object resultIdVal = EsGetterUtil.invokeGetterValue(mapping, dmlData);
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getColumnItems().isEmpty()) {
                continue;
            }
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();


            /*
             * 修复canal的bug.
             * 针对于 select a.name AS aName, b.name AS bName, c.name AS cName from a left join b left join c 的情况.
             * canal会把修改a表,canal会把 b,c表查询的name字段都改了.
             *
             * 修复方式, 只修改本表的字段. aliases.contains(fieldItem.getOwner())
             *
             *  case : 修改职位名称, 项目名称与公司名称与项目BU名称都变成职位名称了.
             * 王子豪 2019年6月4日 17:39:31
             */
            if (fieldItem.containsOwner(aliases)
                    && fieldItem.containsColumnName(dmlOld.keySet())) {
                Object newValue = fieldItem.getValue(dmlData);
                Object oldValue = fieldItem.getValue(dmlOld);
                if (!mapping.isWriteNull() && newValue == null && oldValue == null) {
                    continue;
                }
                esFieldData.put(fieldName,
                        getColumnValueAndMysql2EsType(mapping, dmlData, fieldName, columnName, esTemplate));
            }
        }

        return resultIdVal;
    }

    private static Object getColumnValueAndMysql2EsType(ESMapping mapping, Map<String, Object> dmlData,
                                                        String fieldName, String columnName, ESTemplate esTemplate) {
        return EsTypeUtil.mysql2EsType(mapping, dmlData, dmlData.get(columnName), fieldName, esTemplate.getEsType(mapping), null);
    }

    public WriteResult writeEsReturnSql(Collection<ESSyncConfig> configList, List<Dml> dmlList, ESTemplate.BulkRequestList bulkRequestList) {
        WriteResult result = new WriteResult();
        for (ESSyncConfig config : configList) {
            for (Dml dml : dmlList) {
                try {
                    String type = dml.getType();
                    if (type == null) {
                        type = "INSERT";
                    } else {
                        type = type.toUpperCase();
                    }
                    switch (type) {
                        case "UPDATE":
                            update(config, dml, bulkRequestList, result);
                            break;
                        case "INSERT":
                        case "INIT":
                            insert(config, dml, bulkRequestList, result);
                            break;
                        case "DELETE":
                            delete(config, dml, bulkRequestList, result);
                            break;
                        default:
                            logger.info("sync unkonw type, es index: {}, DML : {}, 异常: {}", type, config.getEsMapping().get_index(), dml);
                            break;
                    }
                } catch (Throwable e) {
                    logger.error("sync error, es index: {}, DML : {}, 异常: {}", config.getEsMapping().get_index(), dml, Util.getStackTrace(e));
                    Util.sneakyThrows(e);
                }
            }
        }
        return result;
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void insert(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList, WriteResult result) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        boolean existTable = schemaItem.existTable(dml.getTable());
        int index;
        int i = 0;
        for (Map<String, Object> data : dataList) {
            index = i++;
            if (data == null || data.isEmpty()) {
                continue;
            }
            boolean add = false;
            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                if (isMainTable(schemaItem, dml.getTable())) {
                    result.insertList.add(singleTableSimpleFiledInsert(config, data, dml, index, bulkRequestList));
                    add = true;
                }
            } else {
                // ------是主表 查询sql来插入------
                if (isMainTable(schemaItem, dml.getTable())) {
                    InsertESSyncConfigSQL sql = mainTableInsert(config, dml, data, index, bulkRequestList);
                    result.sqlList.add(sql);
                    add = true;
                }

                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
                        continue;
                    }
                    // insert的情况下，主键关联不可能存在关联影响 hao 2024-07-12
                    if (tableItem.containsOwnerColumn(dml.getPkNames())) {
                        continue;
                    }

                    // 关联条件出现在主表查询条件是否为简单字段
                    boolean allFieldsSimple = true;
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                            allFieldsSimple = false;
                            break;
                        }
                    }
                    // 所有查询字段均为简单字段
                    if (allFieldsSimple) {
                        // 不是子查询
                        if (!tableItem.isSubQuery()) {
                            // ------关联表简单字段插入------
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                Object value = getColumnValueAndMysql2EsType(config.getEsMapping(), data, fieldItem.getFieldName(), fieldItem.getColumnName(), esTemplate);
                                esFieldData.put(fieldItem.getFieldName(), value);
                            }
                            result.updateList.add(joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, index, bulkRequestList));
                            add = true;
                        } else {
                            // ------关联子表简单字段插入------
                            SubTableSimpleFieldESSyncConfigSQL sql = subTableSimpleFieldOperation(config, dml, data, null, tableItem, index, bulkRequestList);
                            result.sqlList.add(sql);
                            add = true;
                        }
                    } else {
                        // ------关联子表复杂字段插入 执行全sql更新es------
                        WholeSqlOperationESSyncConfigSQL sql = wholeSqlOperation(config, dml, data, null, tableItem, index, bulkRequestList);
                        result.sqlList.add(sql);
                        add = true;
                    }
                }
            }
            if (!add && existTable) {
                result.insertList.add(new SqlDependent(schemaItem, index, dml, Boolean.FALSE));
            }
        }
    }

    private boolean isAllUpdateFieldSimple(Map<String, Object> old, Map<String, FieldItem> selectFields) {
        for (FieldItem fieldItem : selectFields.values()) {
            for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                if (old.containsKey(columnItem.getColumnName())) {
                    if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * 更新操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void update(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList, WriteResult result) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        boolean existTable = schemaItem.existTable(dml.getTable());
        int i = 0;
        for (Map<String, Object> data : dataList) {
            int index = i++;
            Map<String, Object> old = oldList.get(index);
            if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
                continue;
            }
            boolean add = false;
            // 主表
            if (isMainTable(schemaItem, dml.getTable())) {
                if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                    // ------单表 & 所有字段都为简单字段------
                    result.updateList.add(singleTableSimpleFiledUpdate(config, dml, data, old, index, bulkRequestList));
                    add = true;
                } else {
                    // ------主表 查询sql来更新------
                    ESMapping mapping = config.getEsMapping();
                    String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();

                    FieldItem idFieldItem = schemaItem.getSelectFields().get(idFieldName);
                    boolean idFieldSimple = !idFieldItem.isMethod() && !idFieldItem.isBinaryOp();
                    boolean allUpdateFieldSimple = isAllUpdateFieldSimple(old, schemaItem.getSelectFields());

                    // 不支持主键更新!!

                    // 判断是否有外键更新
                    boolean fkChanged = false;
                    for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                        if (tableItem.isMain()) {
                            continue;
                        }
                        boolean changed = false;
                        for (List<FieldItem> fieldItems : tableItem.getRelationTableFields().values()) {
                            for (FieldItem fieldItem : fieldItems) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    fkChanged = true;
                                    changed = true;
                                    break;
                                }
                            }
                        }
                        // 如果外键有修改,则更新所对应该表的所有查询条件数据
                        if (changed) {
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                fieldItem.getColumnItems().forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                            }
                        }
                    }

                    // 判断主键和所更新的字段是否全为简单字段
                    if (idFieldSimple && allUpdateFieldSimple && !fkChanged) {
                        result.updateList.add(singleTableSimpleFiledUpdate(config, dml, data, old, index, bulkRequestList));
                    } else {
                        UpdateESSyncConfigSQL sql = mainTableUpdate(config, dml, data, old, index, bulkRequestList);
                        result.sqlList.add(sql);
                    }
                    add = true;
                }
            }

            // 从表的操作
            for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.isMain()) {
                    continue;
                }
                if (!tableItem.getTableName().equals(dml.getTable())) {
                    continue;
                }

                // 关联条件出现在主表查询条件是否为简单字段
                boolean allFieldsSimple = true;
                for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                    if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                        allFieldsSimple = false;
                        break;
                    }
                }

                // 所有查询字段均为简单字段
                if (allFieldsSimple) {
                    // 子查询
                    if (tableItem.isSubQuery()) {
                        // ------关联子表简单字段更新------
                        SubTableSimpleFieldESSyncConfigSQL sql = subTableSimpleFieldOperation(config, dml, data, old, tableItem, index, bulkRequestList);
                        result.sqlList.add(sql);
                        add = true;
                    } else {
                        // ------关联表简单字段更新------
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            if (old.containsKey(fieldItem.getColumnName())) {
                                Object value = getColumnValueAndMysql2EsType(config.getEsMapping(), data, fieldItem.getFieldName(), fieldItem.getColumnName(), esTemplate);
                                esFieldData.put(fieldItem.getFieldName(), value);
                            }
                        }
                        result.updateList.add(joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, index, bulkRequestList));
                        add = true;
                    }
                } else {
                    // ------关联子表复杂字段更新 执行全sql更新es------
                    WholeSqlOperationESSyncConfigSQL sql = wholeSqlOperation(config, dml, data, old, tableItem, index, bulkRequestList);
                    result.sqlList.add(sql);
                    add = true;
                }
            }

            if (!add && existTable) {
                result.updateList.add(new SqlDependent(schemaItem, index, dml, Boolean.FALSE));
            }
        }
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void delete(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList, WriteResult result) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        boolean existTable = schemaItem.existTable(dml.getTable());
        int i = 0;
        for (Map<String, Object> data : dataList) {
            int index = i++;
            if (data == null || data.isEmpty()) {
                continue;
            }
            boolean add = false;
            ESMapping mapping = config.getEsMapping();

            // ------是主表------
            if (isMainTable(schemaItem, dml.getTable())) {
                Object pkVal = EsGetterUtil.invokeGetterValue(mapping, data);
                if (pkVal != null && !"".equals(pkVal)) {
                    esTemplate.delete(mapping, pkVal, bulkRequestList);
                    result.deleteList.add(new SqlDependent(schemaItem, index, dml, Boolean.TRUE));
                    add = true;
                }
            }

            // 从表的操作
            for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.isMain()) {
                    continue;
                }
                if (!tableItem.getTableName().equals(dml.getTable())) {
                    continue;
                }

                // 关联条件出现在主表查询条件是否为简单字段
                boolean allFieldsSimple = true;
                for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                    if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                        allFieldsSimple = false;
                        break;
                    }
                }

                // 所有查询字段均为简单字段
                if (allFieldsSimple) {
                    // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // ------关联表简单字段更新为null------
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            esFieldData.put(fieldItem.getFieldName(), null);
                        }
                        result.updateList.add(joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, index, bulkRequestList));
                        add = true;
                    } else {
                        // ------关联子表简单字段更新------
                        SubTableSimpleFieldESSyncConfigSQL sql = subTableSimpleFieldOperation(config, dml, data, null, tableItem, index, bulkRequestList);
                        result.sqlList.add(sql);
                        add = true;
                    }
                } else {
                    // ------关联子表复杂字段更新 执行全sql更新es------
                    WholeSqlOperationESSyncConfigSQL sql = wholeSqlOperation(config, dml, data, null, tableItem, index, bulkRequestList);
                    result.sqlList.add(sql);
                    add = true;
                }
            }

            if (!add && existTable) {
                result.deleteList.add(new SqlDependent(schemaItem, index, dml, Boolean.FALSE));
            }
        }
    }

    /**
     * 单表简单字段insert
     *
     * @param config es配置
     * @param data   单行dml数据
     */
    private SqlDependent singleTableSimpleFiledInsert(ESSyncConfig config, Map<String, Object> data, Dml dml, int index, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = putAllValueAndMysql2EsType(mapping, data, esFieldData, esTemplate);
        esTemplate.insert(mapping, idVal, esFieldData, bulkRequestList);
        return new SqlDependent(config.getEsMapping().getSchemaItem(), index, dml, Boolean.TRUE);
    }

    /**
     * 单表简单字段update
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行data数据
     * @param old    单行old数据
     */
    private SqlDependent singleTableSimpleFiledUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                                      Map<String, Object> old, int index, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = putAllValueAndMysql2EsType(mapping, data, old, esFieldData, dml.getTable(), esTemplate);
        esTemplate.update(mapping, idVal, esFieldData, bulkRequestList);
        return new SqlDependent(config.getEsMapping().getSchemaItem(), index, dml, esFieldData.isEmpty() ? Boolean.FALSE : Boolean.TRUE);
    }

    /**
     * 关联表主表简单字段operation
     *
     * @param config    es配置
     * @param dml       dml信息
     * @param data      单行dml数据
     * @param tableItem 当前表配置
     */
    private SqlDependent joinTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                                       TableItem tableItem, Map<String, Object> esFieldData, int index, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();

        Map<String, Object> paramsTmp = new LinkedHashMap<>();
        for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
            for (FieldItem fieldItem : entry.getValue()) {
                if (fieldItem.getColumnItems().size() == 1) {
                    Object value = getColumnValueAndMysql2EsType(mapping, data, fieldItem.getFieldName(), entry.getKey().getColumnName(), esTemplate);

                    String fieldName = fieldItem.getFieldName();
                    // 判断是否是主键
                    if (fieldName.equals(mapping.get_id())) {
                        fieldName = ESSyncConfig.ES_ID_FIELD_NAME;
                    }
                    paramsTmp.put(fieldName, value);
                }
            }
        }
        boolean effect = !paramsTmp.isEmpty() && !esFieldData.isEmpty();
        if (effect) {
            esTemplate.updateByQuery(config, paramsTmp, esFieldData, bulkRequestList);
        }
        return new SqlDependent(config.getEsMapping().getSchemaItem(), index, dml, effect);
    }

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private InsertESSyncConfigSQL mainTableInsert(ESSyncConfig config, Dml dml, Map<String, Object> data, int index, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();

        SQL sql = ESSyncUtil.convertSqlByMapping(mapping, data);
        return new InsertESSyncConfigSQL(sql, config, dml, data, bulkRequestList, index, esTemplate);
    }

    /**
     * 关联子查询, 主表简单字段operation
     *
     * @param config    es配置
     * @param dml       dml信息
     * @param data      单行dml数据
     * @param old       单行old数据
     * @param tableItem 当前表配置
     */
    private SubTableSimpleFieldESSyncConfigSQL subTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                                                            Map<String, Object> old, TableItem tableItem, int index, ESTemplate.BulkRequestList bulkRequestList) {
        SQL sql = ESSyncUtil.convertSQLBySubQuery(data, old, tableItem);
        return new SubTableSimpleFieldESSyncConfigSQL(sql, config, dml, data, old, index, bulkRequestList, esTemplate, tableItem);
    }

    /**
     * 关联(子查询), 主表复杂字段operation, 全sql执行
     *
     * @param config    es配置
     * @param dml       dml信息
     * @param data      单行dml数据
     * @param tableItem 当前表配置
     */
    private WholeSqlOperationESSyncConfigSQL wholeSqlOperation(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old,
                                                               TableItem tableItem, int index, ESTemplate.BulkRequestList bulkRequestList) {
        SQL sql = ESSyncUtil.convertSQLByMapping(config.getEsMapping(), data, old, tableItem);
        return new WholeSqlOperationESSyncConfigSQL(sql, config, dml, data, old, index, bulkRequestList, esTemplate, tableItem);
    }

    /**
     * 主表(单表)复杂字段update
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private UpdateESSyncConfigSQL mainTableUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old, int index, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        SQL sql = ESSyncUtil.convertSqlByMapping(mapping, data);
        return new UpdateESSyncConfigSQL(sql, config, dml, data, old, bulkRequestList, index, esTemplate);
    }

    public static class WriteResult {
        private final ArrayList<ESSyncConfigSQL> sqlList = new ArrayList<>();
        private final ArrayList<SqlDependent> updateList = new ArrayList<>();
        private final ArrayList<SqlDependent> insertList = new ArrayList<>();
        private final ArrayList<SqlDependent> deleteList = new ArrayList<>();

        public void add(WriteResult result) {
            sqlList.addAll(result.sqlList);
            updateList.addAll(result.updateList);
            insertList.addAll(result.insertList);
            deleteList.addAll(result.deleteList);
        }

        public Set<String> getIndices() {
            Set<String> indices = new LinkedHashSet<>();
            for (ESSyncConfigSQL sql : sqlList) {
                indices.add(sql.getConfig().getEsMapping().get_index());
            }
            for (SqlDependent sqlDependent : updateList) {
                indices.add(sqlDependent.getSchemaItem().getEsMapping().get_index());
            }
            for (SqlDependent sqlDependent : insertList) {
                indices.add(sqlDependent.getSchemaItem().getEsMapping().get_index());
            }
            for (SqlDependent sqlDependent : deleteList) {
                indices.add(sqlDependent.getSchemaItem().getEsMapping().get_index());
            }
            return indices;
        }

        public List<ESSyncConfigSQL> getSqlList() {
            return sqlList;
        }

        public List<SqlDependent> getDeleteList() {
            return deleteList;
        }

        public List<SqlDependent> getInsertList() {
            return insertList;
        }

        public List<SqlDependent> getUpdateList() {
            return updateList;
        }

        public void trimToSize() {
            sqlList.trimToSize();
            deleteList.trimToSize();
            insertList.trimToSize();
            updateList.trimToSize();
        }
    }
}
