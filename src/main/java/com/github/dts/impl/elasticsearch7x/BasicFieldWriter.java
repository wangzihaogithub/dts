package com.github.dts.impl.elasticsearch7x;

import com.github.dts.util.*;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.ColumnItem;
import com.github.dts.util.SchemaItem.FieldItem;
import com.github.dts.util.SchemaItem.TableItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    public void write(Collection<ESSyncConfig> configList, List<Dml> dmlList, ESTemplate.BulkRequestList bulkRequestList) {
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
                            update(config, dml, bulkRequestList);
                            break;
                        case "INSERT":
                        case "INIT":
                            insert(config, dml, bulkRequestList);
                            break;
                        case "DELETE":
                            delete(config, dml, bulkRequestList);
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
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void insert(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    singleTableSimpleFiledInsert(config, dml, data, bulkRequestList);
                }
            } else {
                // ------是主表 查询sql来插入------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    mainTableInsert(config, dml, data, bulkRequestList);
                }

                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equalsIgnoreCase(dml.getTable())) {
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
                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                            }

                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData, bulkRequestList);
                        } else {
                            // ------关联子表简单字段插入------
                            subTableSimpleFieldOperation(config, dml, data, null, tableItem, bulkRequestList);
                        }
                    } else {
                        // ------关联子表复杂字段插入 执行全sql更新es------
                        wholeSqlOperation(config, dml, data, null, tableItem, bulkRequestList);
                    }
                }
            }
        }
    }

    /**
     * 更新操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void update(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        int i = 0;
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

                    boolean idFieldSimple = true;
                    if (idFieldItem.isMethod() || idFieldItem.isBinaryOp()) {
                        idFieldSimple = false;
                    }

                    boolean allUpdateFieldSimple = true;
                    out:
                    for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                        for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                            if (old.containsKey(columnItem.getColumnName())) {
                                if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                                    allUpdateFieldSimple = false;
                                    break out;
                                }
                            }
                        }
                    }

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
                                fieldItem.getColumnItems()
                                        .forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                            }
                        }
                    }

                    // 判断主键和所更新的字段是否全为简单字段
                    if (idFieldSimple && allUpdateFieldSimple && !fkChanged) {
                        singleTableSimpleFiledUpdate(config, dml, data, old, bulkRequestList);
                    } else {
                        mainTableUpdate(config, dml, data, old, bulkRequestList);
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
                            subTableSimpleFieldOperation(config, dml, data, old, tableItem, bulkRequestList);
                        }
                    } else {
                        // ------关联子表复杂字段更新 执行全sql更新es------
                        wholeSqlOperation(config, dml, data, old, tableItem, bulkRequestList);
                    }
                }
            }

            i++;
        }
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void delete(ESSyncConfig config, Dml dml, ESTemplate.BulkRequestList bulkRequestList) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

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

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, id: {}",
                                    config.getDestination(),
                                    dml.getTable(),
                                    mapping.get_index(),
                                    idVal);
                        }
                        esTemplate.delete(mapping, idVal, null, bulkRequestList);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        // FIXME 删除时反查sql为空记录, 无法获获取 id field 值
                        mainTableDelete(config, dml, data, bulkRequestList);
                    }
                } else {
                    FieldItem pkFieldItem = schemaItem.getIdFieldItem(mapping);
                    if (!pkFieldItem.isMethod() && !pkFieldItem.isBinaryOp()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Object pkVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, pk: {}",
                                    config.getDestination(),
                                    dml.getTable(),
                                    mapping.get_index(),
                                    pkVal);
                        }
                        esFieldData.remove(pkFieldItem.getFieldName());
                        esFieldData.keySet().forEach(key -> esFieldData.put(key, null));
                        esTemplate.delete(mapping, pkVal, esFieldData, bulkRequestList);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        mainTableDelete(config, dml, data, bulkRequestList);
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
                        subTableSimpleFieldOperation(config, dml, data, null, tableItem, bulkRequestList);
                    }
                } else {
                    // ------关联子表复杂字段更新 执行全sql更新es------
                    wholeSqlOperation(config, dml, data, null, tableItem, bulkRequestList);
                }
            }
        }
    }

    /**
     * 单表简单字段insert
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private void singleTableSimpleFiledInsert(ESSyncConfig config, Dml dml, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

        if (logger.isTraceEnabled()) {
            logger.trace("Single table insert ot es index, destination:{}, table: {}, index: {}, id: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    idVal);
        }
        esTemplate.insert(mapping, idVal, esFieldData, bulkRequestList);
    }

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private void mainTableInsert(ESSyncConfig config, Dml dml, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table insert ot es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, esFieldData, data);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Main table insert ot es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                    }
                    esTemplate.insert(mapping, idVal, esFieldData, bulkRequestList);
                }
            } catch (Exception e) {
                logger.error("Util.sqlRS 异常：{}", Util.getStackTrace(e));
                Util.sneakyThrows(e);
            }
            return 0;
        });
    }

    private void mainTableDelete(ESSyncConfig config, Dml dml, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table delete es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                Map<String, Object> esFieldData = null;
                if (mapping.getPk() != null) {
                    esFieldData = new LinkedHashMap<>();
                    esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
                    esFieldData.remove(mapping.getPk());
                    for (String key : esFieldData.keySet()) {
                        esFieldData.put(Util.cleanColumn(key), null);
                    }
                }
                while (rs.next()) {
                    Object idVal = esTemplate.getIdValFromRS(mapping, rs);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Main table delete ot es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                    }
                    esTemplate.delete(mapping, idVal, esFieldData, bulkRequestList);
                }
            } catch (Exception e) {
                logger.error("Util.sqlRS 异常：{}", Util.getStackTrace(e));
                Util.sneakyThrows(e);
            }
            return 0;
        });
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
                        fieldName = "_id";
                    }
                    paramsTmp.put(fieldName, value);
                }
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.updateByQuery(config, paramsTmp, esFieldData, bulkRequestList);
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
    private void subTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old, TableItem tableItem, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM (" + tableItem.getSubQuerySql() + ") " + tableItem.getAlias() + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            ESSyncUtil.appendCondition(sql, value, tableItem.getAlias(), columnName);
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), rs -> {
            try {
                boolean onceFlag = false;
                while (rs.next()) {
                    onceFlag = true;
                    Map<String, Object> esFieldData = new LinkedHashMap<>();

                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            out:
                            for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.equalsField(columnItem0.getColumnName())) {
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                                        data);
                                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                                break out;
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            Object val = esTemplate.getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                    data);
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            if (fieldItem.getColumnItems().size() == 1) {
                                Object value = esTemplate.getValFromRS(mapping, rs, fieldItem.getFieldName(), entry.getKey().getColumnName(),
                                        data);
                                String fieldName = fieldItem.getFieldName();
                                // 判断是否是主键
                                if (fieldName.equals(mapping.get_id())) {
                                    fieldName = "_id";
                                }
                                paramsTmp.put(fieldName, value);
                            }
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData, bulkRequestList);
                }
                if (!onceFlag) {
                    logger.error("有事件，无数据：destination:{}, table: {}, index: {}, sql: {}，data：{},old:{}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            sql.toString().replace("\n", " "), JsonUtil.toJson(data), JsonUtil.toJson(old));
                }
            } catch (Exception e) {
                logger.error("Util.sqlRS 异常：{}", Util.getStackTrace(e));
                Util.sneakyThrows(e);
            }
            return 0;
        });
    }

    /**
     * 关联(子查询), 主表复杂字段operation, 全sql执行
     *
     * @param config    es配置
     * @param dml       dml信息
     * @param data      单行dml数据
     * @param tableItem 当前表配置
     */
    private void wholeSqlOperation(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old,
                                   TableItem tableItem, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        StringBuilder sql = new StringBuilder(mapping.getSql() + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            ESSyncUtil.appendCondition(sql, value, tableItem.getAlias(), columnName);
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query whole sql, destination:{}, table: {}, index: {}, sql: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            // 从表子查询
                            out:
                            for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.getFieldName().equals(columnItem0.getColumnName())) {
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                                        data);
                                                esFieldData.put(fieldItem.getFieldName(), val);
                                                break out;
                                            }
                                        }
                                    }
                                }
                            }
                            // 从表非子查询
                            for (FieldItem fieldItem1 : tableItem.getRelationSelectFieldItems()) {
                                if (fieldItem1.equals(fieldItem)) {
                                    for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                        if (old.containsKey(columnItem.getColumnName())) {
                                            Object val = esTemplate.getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                                    data);
                                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            Object val = esTemplate
                                    .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName(),
                                            data);
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            Object value = esTemplate
                                    .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName(),
                                            data);
                            String fieldName = fieldItem.getFieldName();
                            // 判断是否是主键
                            if (fieldName.equals(mapping.get_id())) {
                                fieldName = "_id";
                            }
                            paramsTmp.put(fieldName, value);
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Join table update es index by query whole sql, destination:{}, table: {}, index: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData, bulkRequestList);
                }
            } catch (Exception e) {
                logger.error("Util.sqlRS 异常：{}", Util.getStackTrace(e));
                Util.sneakyThrows(e);
            }
            return 0;
        });
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

        if (logger.isTraceEnabled()) {
            logger.trace("Main table update ot es index, destination:{}, table: {}, index: {}, id: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    idVal);
        }
        esTemplate.update(mapping, idVal, esFieldData, bulkRequestList);
    }

    /**
     * 主表(单表)复杂字段update
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private void mainTableUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old, ESTemplate.BulkRequestList bulkRequestList) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table update ot es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, old, esFieldData, data);
                    if (idVal == null) {
                        logger.info("空 ==============》 {}", dml);
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Main table update ot es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                    }
                    esTemplate.update(mapping, idVal, esFieldData, bulkRequestList);
                }
            } catch (Exception e) {
                logger.error("Util.sqlRS 异常：{}", Util.getStackTrace(e));
                Util.sneakyThrows(e);
            }
            return 0;
        });
    }

}
