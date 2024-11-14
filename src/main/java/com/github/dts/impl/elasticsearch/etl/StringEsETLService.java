package com.github.dts.impl.elasticsearch.etl;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.elasticsearch.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 根据非自增ID的全量灌数据，可以继承这个Controller
 * <pre>
 * id	                        group_name	    name
 * 2024071120001536320040986	陕西煤业化工集团	海南德璟置业投资有限责任公司
 * 2024071120001540020040987	陕西煤业化工集团	西安重装渭南橡胶制品有限公司
 * 2024071120001546920040988	仁怀市建工集团	仁怀城投中资智慧城市运营有限公司
 * 2024071120001563920040989	苏州城市建设投资发展集团	苏州物资控股（集团）有限责任公司
 * </pre>
 * <pre>
 * curl "<a href="http://localhost:8080/es/myxxx/syncById?id=2024071120255559720056013,2024071118325561520000001">http://localhost:8080/es/myxxx/syncById?id=2024071120255559720056013,2024071118325561520000001</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/syncAll">http://localhost:8080/es/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/stop">http://localhost:8080/es/myxxx/stop</a>"
 * </pre>
 */
public class StringEsETLService {
    private static final Logger log = LoggerFactory.getLogger(StringEsETLService.class);
    protected final StartupServer startupServer;
    private final ExecutorService executorService;
    private final String name;
    private boolean stop = false;

    public StringEsETLService(String name, StartupServer startupServer) {
        this(name, startupServer, 1000);
    }

    public StringEsETLService(String name, StartupServer startupServer, int threads) {
        this.name = name;
        this.executorService = Util.newFixedThreadPool(threads, 5000L,
                name, true);
        this.startupServer = startupServer;
    }

    public void stopSync() {
        stop = true;
    }

    protected List<ESAdapter> getAdapterList(List<String> adapterNames) {
        List<ESAdapter> adapterList = startupServer.getAdapter(ESAdapter.class);
        if (adapterNames == null || adapterNames.isEmpty()) {
            return adapterList;
        } else {
            return adapterList.stream()
                    .filter(e -> adapterNames.contains(e.getConfiguration().getName()))
                    .collect(Collectors.toList());
        }
    }

    public void syncAll(
            String esIndexName) {
        syncAll(esIndexName, "0", 500, true, true, 100, null, null, null);
    }

    public Object syncById(String[] id,
                           String esIndexName) {
        return syncById(id, esIndexName, true, null, null);
    }

    public int updateEsDiff(String esIndexName) {
        return updateEsDiff(esIndexName, 1000, null, 500, null);
    }

    public int deleteEsTrim(String esIndexName) {
        return deleteEsTrim(esIndexName, 1000, 100, null);
    }

    public int syncAll(
            String esIndexName,
            String offsetStart,
            int offsetAdd,
            boolean append,
            boolean onlyCurrentIndex,
            int joinUpdateSize,
            Set<String> onlyFieldNameSet,
            List<String> adapterNames,
            String sqlWhere) {
        String trimWhere = ESSyncUtil.trimWhere(sqlWhere);
        this.stop = false;
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (ESAdapter adapter : adapterList) {
            count += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (count == 0) {
            return 0;
        }
        AbstractMessageService messageService = startupServer.getMessageService();
        for (ESAdapter adapter : adapterList) {
            executorService.execute(() -> {
                Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                for (ESSyncConfig config : configMap.values()) {
                    JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                    String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());

                    String minId = offsetStart;
                    Date timestamp = new Timestamp(System.currentTimeMillis());
                    AtomicInteger dmlSize = new AtomicInteger(0);
                    try {
                        List<Dml> list;
                        do {
                            if (stop) {
                                break;
                            }
                            list = syncAll(jdbcTemplate, catalog, minId, offsetAdd, append, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet, adapter, config, trimWhere);
                            dmlSize.addAndGet(list.size());
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.intValue(), minId);
                            }
                            minId = getDmlListMaxId(list);
                        } while (minId != null);
                        sendDone(messageService, timestamp, dmlSize.intValue(), onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere);
                    } catch (Exception e) {
                        sendError(messageService, e, minId, timestamp, dmlSize.get(), onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere);
                        throw e;
                    }
                }
            });
        }
        return count;
    }

    public int syncById(String[] id,
                        String esIndexName,
                        boolean onlyCurrentIndex,
                        Set<String> onlyFieldNameSet,
                        List<String> adapterNames) {
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (ESAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                try {
                    count += id.length;
                    syncById(jdbcTemplate, catalog, Arrays.asList(id), onlyCurrentIndex, onlyFieldNameSet, adapter, config);
                } finally {
                    log.info("all sync end.  total = {} ", count);
                }
            }
        }
        return count;
    }

    public int deleteEsTrim(String esIndexName,
                            int offsetAdd,
                            int maxSendMessageDeleteIdSize,
                            List<String> adapterNames) {
        this.stop = false;
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return 0;
        }
        int r = 0;
        for (ESAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return r;
        }

        AbstractMessageService messageService = startupServer.getMessageService();
        for (ESAdapter adapter : adapterList) {
            executorService.execute(() -> {
                int hitListSize = 0;
                int deleteSize = 0;
                List<String> deleteIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                try {
                    Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                    for (ESSyncConfig config : configMap.values()) {
                        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
                        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                        String pk = esMapping.getPk();
                        String tableName = esMapping.getSchemaItem().getMainTable().getTableName();

                        ESTemplate esTemplate = adapter.getEsTemplate();

                        Object[] searchAfter = null;
                        do {
                            if (stop) {
                                break;
                            }
                            ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfterId(esMapping, searchAfter, offsetAdd);
                            List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                            if (hitList.isEmpty()) {
                                break;
                            }
                            hitListSize += hitList.size();
                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining("','"));
                            String sql = String.format("select %s from %s where %s in ('%s'')", pk, tableName, pk, ids);
                            Set<String> dbIds = new HashSet<>(jdbcTemplate.queryForList(sql, String.class));
                            for (ESTemplate.Hit hit : hitList) {
                                String id = hit.getId();
                                if (!dbIds.contains(id)) {
                                    esTemplate.delete(esMapping, id, null);
                                    deleteSize++;
                                    if (deleteIdList.size() < maxSendMessageDeleteIdSize) {
                                        deleteIdList.add(id);
                                    }
                                }
                            }
                            esTemplate.commit();
                            searchAfter = searchResponse.getLastSortValues();
                            log.info("deleteEsTrim hits={}, searchAfter = {}", hitListSize, searchAfter);
                        } while (true);
                        sendTrimDone(messageService, timestamp, hitListSize, deleteSize, deleteIdList, config, adapter);
                    }
                } catch (Exception e) {
                    sendTrimError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, esIndexName, adapter);
                }
            });
        }
        return r;
    }

    public int updateEsDiff(String esIndexName,
                            int offsetAdd,
                            Set<String> diffFields,
                            int maxSendMessageSize,
                            List<String> adapterNames) {
        this.stop = false;
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return 0;
        }
        int r = 0;
        for (ESAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return r;
        }

        AbstractMessageService messageService = startupServer.getMessageService();
        for (ESAdapter adapter : adapterList) {
            executorService.execute(() -> {
                long hitListSize = 0;
                long deleteSize = 0;
                long updateSize = 0;
                List<String> deleteIdList = new ArrayList<>();
                List<String> updateIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                ESSyncConfig lastConfig = null;
                try {
                    Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                    for (ESSyncConfig config : configMap.values()) {
                        lastConfig = config;
                        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
                        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                        String pkFieldName = config.getEsMapping().getSchemaItem().getIdField().getFieldName();
                        String pkFieldExpr = config.getEsMapping().getSchemaItem().getIdField().getOwnerAndColumnName();
                        String[] selectFields = esMapping.getSchemaItem().getSelectFields().keySet().toArray(new String[0]);

                        DefaultESTemplate esTemplate = adapter.getEsTemplate();

                        Object[] searchAfter = null;
                        do {
                            if (stop) {
                                break;
                            }
                            ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfter(esMapping, selectFields, null, searchAfter, offsetAdd);
                            List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                            if (hitList.isEmpty()) {
                                break;
                            }
                            hitListSize += hitList.size();

                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining("','"));
                            String sql = String.format("%s where %s in ('%s') group by %s", esMapping.getSql(), pkFieldExpr, ids, pkFieldExpr);
                            Map<String, Map<String, Object>> dbMap = jdbcTemplate.queryForList(sql).stream()
                                    .collect(Collectors.toMap(e -> String.valueOf(e.get(pkFieldName)), Function.identity()));

                            for (ESTemplate.Hit hit : hitList) {
                                String id = hit.getId();
                                Map<String, Object> db = dbMap.get(id);
                                if (db != null) {
                                    if (!ESSyncUtil.equalsRowData(db, hit, diffFields, esMapping)) {
                                        updateSize++;
                                        if (updateIdList.size() < maxSendMessageSize) {
                                            updateIdList.add(id);
                                        }
                                        esTemplate.update(esMapping, id, db, null);
                                    }
                                } else {
                                    esTemplate.delete(esMapping, id, null);
                                    deleteSize++;
                                    if (deleteIdList.size() < maxSendMessageSize) {
                                        deleteIdList.add(id);
                                    }
                                }
                            }
                            esTemplate.commit();
                            searchAfter = searchResponse.getLastSortValues();
                            log.info("updateEsDiff hits={}, searchAfter = {}", hitListSize, searchAfter);
                        } while (true);
                        sendDiffDone(messageService, timestamp, hitListSize, deleteSize, deleteIdList, updateSize, updateIdList, diffFields, adapter, config);
                    }
                } catch (Exception e) {
                    sendDiffError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, updateSize, updateIdList, diffFields, adapter, lastConfig);
                }
            });
        }
        return r;
    }

    public List discard(String clientIdentity) throws InterruptedException {
        List list = new ArrayList();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(clientIdentity)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    protected List<Dml> syncAll(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit, boolean append, boolean onlyCurrentIndex,
                                int joinUpdateSize, Collection<String> onlyFieldNameSet,
                                ESAdapter esAdapter, ESSyncConfig config, String where) {
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        String pk = config.getEsMapping().getPk();
        List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, minId, limit, tableName, pk, config, where);
        if (dmlList.isEmpty()) {
            return dmlList;
        }
        if (!append) {
            String dmlListMaxId = getDmlListMaxId(dmlList);
            ESBulkRequest.ESBulkResponse esBulkResponse = esAdapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, minId, dmlListMaxId, limit);
        }
        esAdapter.sync(dmlList, false, false, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet, null);
        return dmlList;
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<String> id, boolean onlyCurrentIndex, Collection<String> onlyFieldNameSet,
                           ESAdapter esAdapter, ESSyncConfig config) {
        this.stop = false;
        if (id == null || id.isEmpty()) {
            return 0;
        }
        int count = 0;
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        String pk = config.getEsMapping().getPk();
        for (String i : id) {
            if (stop) {
                break;
            }
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1, tableName, pk, config, null);
            for (Dml dml : dmlList) {
                dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
            }
            esAdapter.sync(dmlList, false, false, onlyCurrentIndex, 1, onlyFieldNameSet, null);
            count += dmlList.size();
        }
        return count;
    }

    public int updateEsNestedDiff(String esIndexName) {
        return updateEsNestedDiff(esIndexName, null, 500, null, 1000, null);
    }

    public int updateEsNestedDiff(String esIndexName,
                                  String startId,
                                  int offsetAdd,
                                  Set<String> diffFields,
                                  int maxSendMessageSize,
                                  List<String> adapterNames) {
        this.stop = false;
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return 0;
        }
        int r = 0;
        for (ESAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return r;
        }

        int maxIdInCount = 1000;
        AbstractMessageService messageService = startupServer.getMessageService();
        for (ESAdapter adapter : adapterList) {
            DefaultESTemplate esTemplate = adapter.getEsTemplate();
            AtomicBoolean done = new AtomicBoolean(false);
            AtomicBoolean commit = new AtomicBoolean(false);
            executorService.execute(() -> {
                while (true) {
                    if (commit.compareAndSet(false, true)) {
                        synchronized (commit) {
                            esTemplate.commit();
                        }
                    }
                    if (done.get()) {
                        esTemplate.commit();
                        break;
                    }
                    Thread.yield();
                }
            });
            executorService.execute(() -> {
                String lastId = null;
                long hitListSize = 0;
                long updateSize = 0;
                List<String> updateIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                ESSyncConfig lastConfig = null;
                Set<String> diffFieldsFinal = diffFields;
                try {
                    Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                    for (ESSyncConfig config : configMap.values()) {
                        lastConfig = config;
                        ESSyncConfig.ESMapping esMapping = config.getEsMapping();

                        String pkFieldName = config.getEsMapping().getSchemaItem().getIdField().getFieldName();
                        String pkFieldExpr = config.getEsMapping().getSchemaItem().getIdField().getOwnerAndColumnName();
                        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());

                        if (diffFields == null || diffFields.isEmpty()) {
                            diffFieldsFinal = esMapping.getObjFields().entrySet().stream().filter(e -> e.getValue().getType().isSqlType()).map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedHashSet::new));
                        } else {
                            diffFieldsFinal = diffFields;
                        }
                        int uncommit = 0;
                        Object[] searchAfter = startId == null ? null : new Object[]{startId};
                        do {
                            if (stop) {
                                break;
                            }
                            ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfter(esMapping, diffFieldsFinal.toArray(new String[0]), null, searchAfter, offsetAdd);
                            List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                            if (hitList.isEmpty()) {
                                break;
                            }
                            hitListSize += hitList.size();
                            Set<String> select = new LinkedHashSet<>();
                            for (String diffField : diffFieldsFinal) {
                                ESSyncConfig.ObjectField objectField = esMapping.getObjectField(null, diffField);
                                if (objectField == null) {
                                    continue;
                                }
                                Set<String> cols = SQL.convertToSql(objectField.getParamSql().getFullSql(true), Collections.emptyMap()).getArgsMap().keySet();
                                for (String col : cols) {
                                    select.add("`" + col + "`");
                                }
                            }
                            select.add("`" + pkFieldName + "`");
                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining("','"));
                            String sql = String.format("select %s from %s where %s in ('%s') group by %s",
                                    String.join(",", select),
                                    esMapping.getSchemaItem().getMainTable().getTableName(), pkFieldExpr, ids, pkFieldExpr);
                            Map<String, Map<String, Object>> db1Map = jdbcTemplate.queryForList(sql).stream()
                                    .collect(Collectors.toMap(e -> String.valueOf(e.get(pkFieldName)), Function.identity()));

                            Map<String, List<EsJdbcTemplateSQL>> jdbcTemplateSQLList = new HashMap<>();
                            for (String diffField : diffFieldsFinal) {
                                List<EsJdbcTemplateSQL> list = new ArrayList<>();
                                for (ESTemplate.Hit hit : hitList) {
                                    ESSyncConfig.ObjectField objectField = esMapping.getObjectField(null, diffField);
                                    if (objectField == null) {
                                        continue;
                                    }
                                    Map<String, Object> map = db1Map.get(hit.getId());
                                    if (map == null) {
                                        continue;
                                    }
                                    SQL sql1 = SQL.convertToSql(objectField.getParamSql().getFullSql(true), map);
                                    EsJdbcTemplateSQL jdbcTemplateSQL = new EsJdbcTemplateSQL(sql1.getExprSql(), sql1.getArgs(), map, config.getDataSourceKey(),
                                            objectField.getParamSql().getSchemaItem().getGroupByIdColumns(), hit);
                                    list.add(jdbcTemplateSQL);
                                }
                                jdbcTemplateSQLList.put(diffField, list);
                            }

                            Map<String, Future<Map<String, List<Map<String, Object>>>>> fieldDbMap = new HashMap<>(jdbcTemplateSQLList.size());
                            if (jdbcTemplateSQLList.size() == 1) {
                                for (Map.Entry<String, List<EsJdbcTemplateSQL>> entry : jdbcTemplateSQLList.entrySet()) {
                                    List<MergeJdbcTemplateSQL<EsJdbcTemplateSQL>> mergeList = MergeJdbcTemplateSQL.merge(entry.getValue(), maxIdInCount);
                                    fieldDbMap.put(entry.getKey(), CompletableFuture.completedFuture(MergeJdbcTemplateSQL.toMap(mergeList, e -> e.getHit().getId())));
                                }
                            } else {
                                for (Map.Entry<String, List<EsJdbcTemplateSQL>> entry : jdbcTemplateSQLList.entrySet()) {
                                    fieldDbMap.put(entry.getKey(), executorService.submit(() -> {
                                        List<MergeJdbcTemplateSQL<EsJdbcTemplateSQL>> mergeList = MergeJdbcTemplateSQL.merge(entry.getValue(), maxIdInCount);
                                        return MergeJdbcTemplateSQL.toMap(mergeList, e -> e.getHit().getId());
                                    }));
                                }
                            }
                            for (String field : jdbcTemplateSQLList.keySet()) {
                                ESSyncConfig.ObjectField objectField = esMapping.getObjectField(null, field);
                                Map<String, List<Map<String, Object>>> dbMap = fieldDbMap.get(field).get();
                                for (ESTemplate.Hit hit : hitList) {
                                    String id = hit.getId();
                                    lastId = id;
                                    Object esData = hit.get(field);
                                    List<Map<String, Object>> mysqlData = dbMap.get(id);
                                    if (ESSyncUtil.equalsNestedRowData(mysqlData, esData, objectField)) {
                                        continue;
                                    }
                                    updateSize++;
                                    if (updateIdList.size() < maxSendMessageSize) {
                                        updateIdList.add(id);
                                    }
                                    Object esUpdateData;
                                    if (objectField.getType() == ESSyncConfig.ObjectField.Type.OBJECT_SQL) {
                                        esUpdateData = ESSyncUtil.convertValueTypeCopyMap(mysqlData, esTemplate, esMapping, objectField.getFieldName());
                                    } else {
                                        esUpdateData = ESSyncUtil.convertValueTypeCopyList(mysqlData, esTemplate, esMapping, objectField.getFieldName());
                                    }
                                    esTemplate.update(esMapping, field, id, Collections.singletonMap(objectField.getFieldName(), esUpdateData), null);

                                    if (++uncommit >= 35) {
                                        uncommit = 0;
                                        commit.set(false);
                                    }
                                }
                            }
                            searchAfter = searchResponse.getLastSortValues();
                            log.info("updateEsNestedDiff hits={}, searchAfter = {}", hitListSize, searchAfter);
                        } while (!Thread.currentThread().isInterrupted());
                        esTemplate.commit();
                        sendNestedDiffDone(messageService, timestamp, hitListSize, updateSize, updateIdList, diffFieldsFinal, adapter, config);
                    }
                } catch (Exception e) {
                    log.error("updateEsNestedDiff error {}", e.toString(), e);
                    sendNestedDiffError(messageService, e, timestamp, hitListSize, updateSize, updateIdList, diffFieldsFinal, adapter, lastConfig, lastId);
                } finally {
                    done.set(true);
                }
            });
        }
        return r;
    }

    protected void sendNestedDiffDone(AbstractMessageService messageService, Date startTime,
                                      long dmlSize,
                                      long updateSize, List<String> updateIdList,
                                      Set<String> diffFields,
                                      ESAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验嵌套Diff数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 比较nested字段 = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID = " + updateIdList;
        messageService.send(title, content);
    }

    protected void sendNestedDiffError(AbstractMessageService messageService, Throwable throwable,
                                       Date timestamp, long dmlSize,
                                       long updateSize, List<String> updateIdList,
                                       Set<String> diffFields,
                                       ESAdapter adapter, ESSyncConfig config,
                                       String lastId) {
        String title = "ES搜索全量校验嵌套Diff数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 比较nested字段 = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 最近的ID = " + lastId
                + ",\n\n 异常 = " + throwable
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID = " + updateIdList
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    protected void sendDone(AbstractMessageService messageService, Date startTime, int dmlSize,
                            Set<String> onlyFieldNameSet, ESAdapter adapter, ESSyncConfig config, boolean onlyCurrentIndex,
                            String sqlWhere) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 限制条件 = " + sqlWhere
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n DML条数 = " + dmlSize;
        messageService.send(title, content);
    }

    protected void sendError(AbstractMessageService messageService, Throwable throwable, String minId,
                             Date timestamp, int dmlSize, Set<String> onlyFieldNameSet, ESAdapter adapter,
                             ESSyncConfig config, boolean onlyCurrentIndex,
                             String sqlWhere) {
        String title = "ES搜索全量刷数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 限制条件 = " + sqlWhere
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n minOffset = " + minId
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 异常 = " + throwable
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    protected void sendTrimDone(AbstractMessageService messageService, Date startTime, int dmlSize, int deleteSize, List<String> deleteIdList, ESSyncConfig config, ESAdapter adapter) {
        String title = "ES搜索全量校验Trim数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 删除ID = " + deleteIdList;
        messageService.send(title, content);
    }

    protected void sendTrimError(AbstractMessageService messageService, Throwable throwable, Date timestamp, int dmlSize, int deleteSize, List<String> deleteIdList, String esIndexName, ESAdapter adapter) {
        String title = "ES搜索全量校验Trim数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + esIndexName
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 删除ID = " + deleteIdList
                + ",\n\n 异常 = " + throwable
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    protected void sendDiffDone(AbstractMessageService messageService, Date startTime,
                                long dmlSize, long deleteSize, List<String> deleteIdList,
                                long updateSize, List<String> updateIdList,
                                Set<String> diffFields,
                                ESAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Diff数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 比较字段(不含nested) = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID = " + updateIdList
                + ",\n\n 删除ID = " + deleteIdList;
        messageService.send(title, content);
    }

    protected void sendDiffError(AbstractMessageService messageService, Throwable throwable,
                                 Date timestamp, long dmlSize, long deleteSize, List<String> deleteIdList,
                                 long updateSize, List<String> updateIdList,
                                 Set<String> diffFields,
                                 ESAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Diff数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 比较字段(不含nested) = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 异常 = " + throwable
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID = " + updateIdList
                + ",\n\n 删除ID = " + deleteIdList
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit, String tableName, String idColumnName, ESSyncConfig config, String where) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName, where);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Arrays.asList(row), Arrays.asList(idColumnName), tableName, catalog, new String[]{config.getDestination()}));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, String minId, int limit, String tableName, String idColumnName, String where) {
        String sql;
        if (limit == 1) {
            sql = "select * from " + tableName + " where " + idColumnName + " = ?";
        } else {
            sql = "select * from " + tableName + " where " + idColumnName + " > ?";
        }
        boolean whereAppend = Util.isNotBlank(where);
        if (whereAppend) {
            sql += " and (" + where + ")";
        }
        sql += " limit ?";
        if (whereAppend) {
            log.info("selectList sql = {}", sql);
        }
        return jdbcTemplate.queryForList(sql, minId, limit);
    }

    protected String getDmlListMaxId(List<Dml> list) {
        if (list.isEmpty()) {
            return null;
        }
        Dml dml = list.get(list.size() - 1);
        List<String> pkNames = dml.getPkNames();
        if (pkNames.size() != 1) {
            throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
        }
        String pkName = pkNames.iterator().next();
        Map<String, Object> last = dml.getData().get(dml.getData().size() - 1);
        Object o = last.get(pkName);
        return o == null || "".equals(o) ? null : o.toString();
    }

    public String getName() {
        return name;
    }
}