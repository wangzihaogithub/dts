package com.github.dts.impl.elasticsearch7x;

import com.github.dts.impl.elasticsearch7x.basic.*;
import com.github.dts.impl.elasticsearch7x.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.*;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.ColumnItem;
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

    private static void executeUpdate(List<ESSyncConfigSQL> sqlList) {
        List<MergeJdbcTemplateSQL<ESSyncConfigSQL>> mergeList = MergeJdbcTemplateSQL.merge(sqlList, 500);
        Map<ESSyncConfigSQL, List<Map<String, Object>>> executedMap = MergeJdbcTemplateSQL.executeQueryList(mergeList, null);
        for (Map.Entry<ESSyncConfigSQL, List<Map<String, Object>>> entry : executedMap.entrySet()) {
            ESSyncConfigSQL sql = entry.getKey();
            sql.run(entry.getValue());
        }
    }

    public void write(Collection<ESSyncConfig> configList, List<Dml> dmlList, ESTemplate.BulkRequestList bulkRequestList) {
        List<ESSyncConfigSQL> sqlList = writeEsReturnSql(configList, dmlList, bulkRequestList);
        executeUpdate(sqlList);
    }

    private List<ESSyncConfigSQL> writeEsReturnSql(Collection<ESSyncConfig> configList, List<Dml> dmlList, ESTemplate.BulkRequestList bulkRequestList) {
        List<ESSyncConfigSQL> sqlList = new ArrayList<>();
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
                            sqlList.addAll(update(config, dml, bulkRequestList));
                            break;
                        case "INSERT":
                        case "INIT":
                            sqlList.addAll(insert(config, dml, bulkRequestList));
                            break;
                        case "DELETE":
                            sqlList.addAll(delete(config, dml, bulkRequestList));
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
        return sqlList;
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private List<ESSyncConfigSQL> insert(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return Collections.emptyList();
        }
        List<ESSyncConfigSQL> list = new ArrayList<>();
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    singleTableSimpleFiledInsert(config, data, bulkRequestList);
                }
            } else {
                // ------是主表 查询sql来插入------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    InsertESSyncConfigSQL sql = mainTableInsert(config, dml, data, bulkRequestList);
                    list.add(sql);
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
                                Object value = esTemplate.getValFromData(config.getEsMapping(), data, fieldItem.getFieldName(), fieldItem.getColumnName());
                                esFieldData.put(fieldItem.getFieldName(), value);
                            }

                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, bulkRequestList);
                        } else {
                            // ------关联子表简单字段插入------
                            SubTableSimpleFieldESSyncConfigSQL sql = subTableSimpleFieldOperation(config, dml, data, null, tableItem, bulkRequestList);
                            list.add(sql);
                        }
                    } else {
                        // ------关联子表复杂字段插入 执行全sql更新es------
                        WholeSqlOperationESSyncConfigSQL sql = wholeSqlOperation(config, dml, data, null, tableItem, bulkRequestList);
                        list.add(sql);
                    }
                }
            }
        }
        return list;
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
    private List<ESSyncConfigSQL> update(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return Collections.emptyList();
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        int i = 0;
        List<ESSyncConfigSQL> list = new ArrayList<>();
        for (Map<String, Object> data : dataList) {
            Map<String, Object> old = oldList.get(i);
            if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                singleTableSimpleFiledUpdate(config, dml, data, old, bulkRequestList);
            } else {
                // ------主表 查询sql来更新------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
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
                        singleTableSimpleFiledUpdate(config, dml, data, old, bulkRequestList);
                    } else {
                        UpdateESSyncConfigSQL sql = mainTableUpdate(config, dml, data, old, bulkRequestList);
                        list.add(sql);
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
                            // ------关联表简单字段更新------
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                if (old.containsKey(fieldItem.getColumnName())) {
                                    Object value = esTemplate.getValFromData(config.getEsMapping(), data, fieldItem.getFieldName(), fieldItem.getColumnName());
                                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                                }
                            }
                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, bulkRequestList);
                        } else {
                            // ------关联子表简单字段更新------
                            SubTableSimpleFieldESSyncConfigSQL sql = subTableSimpleFieldOperation(config, dml, data, old, tableItem, bulkRequestList);
                            list.add(sql);
                        }
                    } else {
                        // ------关联子表复杂字段更新 执行全sql更新es------
                        WholeSqlOperationESSyncConfigSQL sql = wholeSqlOperation(config, dml, data, old, tableItem, bulkRequestList);
                        list.add(sql);
                    }
                }
            }

            i++;
        }
        return list;
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private List<ESSyncConfigSQL> delete(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return Collections.emptyList();
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        List<ESSyncConfigSQL> list = new ArrayList<>();
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            ESMapping mapping = config.getEsMapping();

            // ------是主表------
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                if (mapping.get_id() != null) {
                    FieldItem idFieldItem = schemaItem.getIdFieldItem(mapping);
                    // 主键为简单字段
                    if (!idFieldItem.isMethod() && !idFieldItem.isBinaryOp()) {
                        Object idVal = esTemplate.getValFromData(mapping, data, idFieldItem.getFieldName(), idFieldItem.getColumnName());
                        esTemplate.delete(mapping, idVal, null, bulkRequestList);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        // FIXME 删除时反查sql为空记录, 无法获获取 id field 值
                        DeleteESSyncConfigSQL sql = mainTableDelete(config, dml, data, bulkRequestList);
                        list.add(sql);
                    }
                } else {
                    FieldItem pkFieldItem = schemaItem.getIdFieldItem(mapping);
                    if (!pkFieldItem.isMethod() && !pkFieldItem.isBinaryOp()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Object pkVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

                        esFieldData.remove(pkFieldItem.getFieldName());
                        esFieldData.keySet().forEach(key -> esFieldData.put(key, null));
                        esTemplate.delete(mapping, pkVal, esFieldData, bulkRequestList);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        DeleteESSyncConfigSQL sql = mainTableDelete(config, dml, data, bulkRequestList);
                        list.add(sql);
                    }
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
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), null);
                        }
                        joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, bulkRequestList);
                    } else {
                        // ------关联子表简单字段更新------
                        SubTableSimpleFieldESSyncConfigSQL sql = subTableSimpleFieldOperation(config, dml, data, null, tableItem, bulkRequestList);
                        list.add(sql);
                    }
                } else {
                    // ------关联子表复杂字段更新 执行全sql更新es------
                    WholeSqlOperationESSyncConfigSQL sql = wholeSqlOperation(config, dml, data, null, tableItem, bulkRequestList);
                    list.add(sql);
                }
            }
        }
        return list;
    }

    /**
     * 单表简单字段insert
     *
     * @param config es配置
     * @param data   单行dml数据
     */
    private void singleTableSimpleFiledInsert(ESSyncConfig config, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
        esTemplate.insert(mapping, idVal, esFieldData, bulkRequestList);
    }

    /**
     * 单表简单字段update
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行data数据
     * @param old    单行old数据
     */
    private void singleTableSimpleFiledUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, old, esFieldData, dml.getTable());
        esTemplate.update(mapping, idVal, esFieldData, bulkRequestList);
    }

    /**
     * 关联表主表简单字段operation
     *
     * @param config    es配置
     * @param dml       dml信息
     * @param data      单行dml数据
     * @param tableItem 当前表配置
     */
    private void joinTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                               TableItem tableItem, Map<String, Object> esFieldData, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();

        Map<String, Object> paramsTmp = new LinkedHashMap<>();
        for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
            for (FieldItem fieldItem : entry.getValue()) {
                if (fieldItem.getColumnItems().size() == 1) {
                    Object value = esTemplate.getValFromData(mapping, data, fieldItem.getFieldName(), entry.getKey().getColumnName());

                    String fieldName = fieldItem.getFieldName();
                    // 判断是否是主键
                    if (fieldName.equals(mapping.get_id())) {
                        fieldName = ESSyncConfig.ES_ID_FIELD_NAME;
                    }
                    paramsTmp.put(fieldName, value);
                }
            }
        }
        esTemplate.updateByQuery(config, paramsTmp, esFieldData, bulkRequestList);
    }

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private InsertESSyncConfigSQL mainTableInsert(ESSyncConfig config, Dml dml, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        SQL sql = ESSyncUtil.convertSqlByMapping(mapping, data);
        return new InsertESSyncConfigSQL(sql, config, dml, data, bulkRequestList, esTemplate);
    }

    private DeleteESSyncConfigSQL mainTableDelete(ESSyncConfig config, Dml dml, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        SQL sql = ESSyncUtil.convertSqlByMapping(mapping, data);
        return new DeleteESSyncConfigSQL(sql, config, dml, data, bulkRequestList, esTemplate);
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
                                                                            Map<String, Object> old, TableItem tableItem, ESTemplate.BulkRequestList bulkRequestList) {
        SQL sql = ESSyncUtil.convertSQLBySubQuery(data, old, tableItem);
        return new SubTableSimpleFieldESSyncConfigSQL(sql, config, dml, data, old, bulkRequestList, esTemplate, tableItem);
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
                                                               TableItem tableItem, ESTemplate.BulkRequestList bulkRequestList) {
        SQL sql = ESSyncUtil.convertSQLByMapping(config.getEsMapping(), data, old, tableItem);
        return new WholeSqlOperationESSyncConfigSQL(sql, config, dml, data, old, bulkRequestList, esTemplate, tableItem);
    }

    /**
     * 主表(单表)复杂字段update
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private UpdateESSyncConfigSQL mainTableUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        SQL sql = ESSyncUtil.convertSqlByMapping(mapping, data);
        return new UpdateESSyncConfigSQL(sql, config, dml, data, old, bulkRequestList, esTemplate);
    }


}
