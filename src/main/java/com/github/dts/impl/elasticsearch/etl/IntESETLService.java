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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 根据自增ID的全量灌数据，可以继承这个Controller
 * <pre>
 * curl "<a href="http://localhost:8080/es/myxxx/syncById?id=1,2">http://localhost:8080/es/myxxx/syncById?id=1,2</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/syncAll">http://localhost:8080/es/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/stop">http://localhost:8080/es/myxxx/stop</a>"
 * </pre>
 */
public class IntESETLService {
    private static final Logger log = LoggerFactory.getLogger(IntESETLService.class);
    protected final StartupServer startupServer;
    private final ExecutorService executorService;
    private final String name;
    private boolean stop = false;

    public IntESETLService(String name, StartupServer startupServer) {
        this(name, startupServer, 1000);
    }

    public IntESETLService(String name, StartupServer startupServer, int threads) {
        this.name = name;
        this.startupServer = startupServer;
        this.executorService = Util.newFixedThreadPool(threads, 5000L,
                name, true);
    }

    public void stopSync() {
        stop = true;
    }

    public List<SyncRunnable> syncAll(String esIndexName) {
        return syncAll(esIndexName,
                50, 0, null, 500,
                true, false, true, 100, null, null, null, false);
    }

    public Object syncById(Long[] id,
                           String esIndexName) {
        return syncById(id, esIndexName, true, null, null);
    }

    public int updateEsDiff(String esIndexName) {
        return updateEsDiff(esIndexName, null, null, 1000, null, 500, null);
    }

    public int deleteEsTrim(String esIndexName) {
        return deleteEsTrim(esIndexName, null, null, 1000, 1000, null);
    }

    public int deleteEsTrim(String esIndexName,
                            Long startId,
                            Long endId,
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
                long hitListSize = 0;
                long deleteSize = 0;
                List<String> deleteIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                ESSyncConfig lastConfig = null;
                String lastId = null;
                try {
                    Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                    for (ESSyncConfig config : configMap.values()) {
                        lastConfig = config;
                        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
                        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                        String pk = esMapping.getPk();
                        String tableName = esMapping.getSchemaItem().getMainTable().getTableName();

                        ESTemplate esTemplate = adapter.getEsTemplate();

                        Object[] searchAfter = startId == null ? null : new Object[]{startId - 1L};
                        do {
                            if (stop) {
                                break;
                            }
                            if (endId != null
                                    && searchAfter != null && searchAfter.length > 0
                                    && searchAfter[0] != null
                                    && Long.parseLong(searchAfter[0].toString()) >= endId) {
                                break;
                            }
                            ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfterId(esMapping, searchAfter, offsetAdd);
                            List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                            if (hitList.isEmpty()) {
                                break;
                            }
                            hitListSize += hitList.size();
                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining(","));
                            String sql = String.format("select %s from %s where %s in (%s)", pk, tableName, pk, ids);
                            Set<String> dbIds = new HashSet<>(jdbcTemplate.queryForList(sql, String.class));
                            for (ESTemplate.Hit hit : hitList) {
                                String id = hit.getId();
                                lastId = id;
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
                        sendTrimDone(messageService, timestamp, hitListSize, deleteSize, deleteIdList, adapter, config);
                    }
                } catch (Exception e) {
                    sendTrimError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, adapter, lastConfig, lastId);
                }
            });
        }
        return r;
    }

    public int updateEsDiff(String esIndexName,
                            Long startId,
                            Long endId,
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
                String lastId = null;
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

                        Object[] searchAfter = startId == null ? null : new Object[]{startId - 1L};
                        do {
                            if (stop) {
                                break;
                            }
                            if (endId != null
                                    && searchAfter != null && searchAfter.length > 0
                                    && searchAfter[0] != null
                                    && Long.parseLong(searchAfter[0].toString()) >= endId) {
                                break;
                            }
                            ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfter(esMapping, selectFields, null, searchAfter, offsetAdd);
                            List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                            if (hitList.isEmpty()) {
                                break;
                            }
                            hitListSize += hitList.size();

                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining(","));
                            String sql = String.format("%s where %s in (%s) group by %s", esMapping.getSql(), pkFieldExpr, ids, pkFieldExpr);
                            Map<String, Map<String, Object>> dbMap = jdbcTemplate.queryForList(sql).stream()
                                    .collect(Collectors.toMap(e -> String.valueOf(e.get(pkFieldName)), Function.identity()));
                            for (ESTemplate.Hit hit : hitList) {
                                String id = hit.getId();
                                lastId = id;
                                Map<String, Object> db = dbMap.get(id);
                                if (db != null) {
                                    if (!ESSyncUtil.equalsRowData(db, hit, diffFields, esMapping)) {
                                        updateSize++;
                                        if (updateIdList.size() < maxSendMessageSize) {
                                            updateIdList.add(id);
                                        }
                                        Map<String, Object> esUpdateData = ESSyncUtil.convertValueTypeCopyMap(db, esTemplate, esMapping, null);
                                        esTemplate.update(esMapping, id, esUpdateData, null);
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
                    sendDiffError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, updateSize, updateIdList, diffFields, adapter, lastConfig, lastId);
                }
            });
        }
        return r;
    }

    public int updateEsNestedDiff(String esIndexName) {
        return updateEsNestedDiff(esIndexName, null, null, 1000, null, 1000, null);
    }

    public int updateEsNestedDiff(String esIndexName,
                                  Long startId,
                                  Long endId,
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
                            diffFieldsFinal = esMapping.getObjFields().entrySet().stream().filter(e -> e.getValue().isSqlType()).map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedHashSet::new));
                        } else {
                            diffFieldsFinal = diffFields;
                        }
                        int uncommit = 0;
                        Object[] searchAfter = startId == null ? null : new Object[]{startId - 1L};
                        do {
                            if (stop) {
                                break;
                            }
                            if (endId != null
                                    && searchAfter != null && searchAfter.length > 0
                                    && searchAfter[0] != null
                                    && Long.parseLong(searchAfter[0].toString()) >= endId) {
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
                                ESSyncConfig.ObjectField.ParamLlmVector paramLlmVector = objectField.getParamLlmVector();
                                if (paramLlmVector != null) {
                                    String etlEqualsFieldName = paramLlmVector.getEtlEqualsFieldName();
                                    if (Util.isNotBlank(etlEqualsFieldName)) {
                                        select.add("`" + etlEqualsFieldName + "`");
                                    }
                                }
                            }
                            select.add("`" + pkFieldName + "`");
                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining(","));
                            String sql = String.format("select %s from %s where %s in (%s) group by %s",
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
                                    Object esUpdateData = ESSyncUtil.convertEsUpdateData(objectField, mysqlData, esTemplate, esMapping);
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

    public List<SyncRunnable> syncAll(
            String esIndexName,
            int threads,
            long offsetStart,
            Long offsetEnd,
            int offsetAdd,
            boolean append,
            boolean discard,
            boolean onlyCurrentIndex,
            int joinUpdateSize,
            Set<String> onlyFieldNameSet,
            List<String> adapterNames,
            String sqlWhere,
            boolean insertIgnore) {
        String trimWhere = ESSyncUtil.trimWhere(sqlWhere);
        this.stop = false;
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }

        List<SyncRunnable> runnableList = new ArrayList<>();
        for (ESAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            if (configMap.isEmpty()) {
                continue;
            }
            String clientIdentity = adapter.getClientIdentity();
            if (discard) {
                new Thread(() -> {
                    try {
                        discard(clientIdentity);
                    } catch (InterruptedException e) {
                        log.info("discard {}", e, e);
                    }
                }).start();
            }
            setSuspendEs(true, clientIdentity);

            AbstractMessageService messageService = startupServer.getMessageService();
            AtomicInteger configDone = new AtomicInteger(configMap.values().size());
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                String pk = config.getEsMapping().getPk();
                String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();

                Long maxId = offsetEnd == null || offsetEnd == Long.MAX_VALUE ?
                        selectMaxId(jdbcTemplate, pk, tableName) : offsetEnd;

                Date timestamp = new Timestamp(System.currentTimeMillis());
                AtomicInteger done = new AtomicInteger(threads);
                AtomicLong dmlSize = new AtomicLong(0);
                for (int i = 0; i < threads; i++) {
                    runnableList.add(new SyncRunnable(name, this,
                            i, offsetStart, maxId, threads, onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere, insertIgnore) {
                        @Override
                        public long run0(long offset) {
                            if (stop) {
                                return Long.MAX_VALUE;
                            }
                            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, offset, offsetAdd, tableName, pk, config, trimWhere);
                            Long dmlListMaxId = getDmlListMaxId(dmlList);
                            List<Dml> filterDmlList;
                            if (insertIgnore) {
                                filterDmlList = filter(dmlList, adapter.getEsTemplate(), config.getEsMapping());
                            } else {
                                filterDmlList = dmlList;
                            }
                            if (!append) {
                                adapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, offset, dmlListMaxId, offsetAdd);
                            }
                            if (!filterDmlList.isEmpty()) {
                                adapter.sync(filterDmlList, false, false, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet, null);
                            }
                            dmlSize.addAndGet(filterDmlList.size());
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.longValue(), SyncRunnable.minOffset(runnableList));
                            }
                            return dmlListMaxId == null ? Long.MAX_VALUE : dmlListMaxId;
                        }

                        @Override
                        public void done() {
                            if (done.decrementAndGet() == 0) {
                                if (log.isInfoEnabled()) {
                                    log.info("syncAll done {}", this);
                                }
                                if (configDone.decrementAndGet() == 0) {
                                    setSuspendEs(false, clientIdentity);
                                }
                                sendDone(messageService, runnableList, timestamp, dmlSize.longValue(), onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere, insertIgnore);
                            }
                        }
                    });
                }
                for (SyncRunnable runnable : runnableList) {
                    executorService.execute(runnable);
                }
            }
        }
        return runnableList;
    }

    public int syncById(Long[] id,
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

    public List discard(String clientIdentity) throws InterruptedException {
        List list = new ArrayList();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(clientIdentity)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    private void setSuspendEs(boolean suspend, String clientIdentity) {
        List<StartupServer.ThreadRef> canalThread = startupServer.getCanalThread(clientIdentity);
        for (StartupServer.ThreadRef thread : canalThread) {
            if (suspend) {
                thread.stopThread();
            } else {
                thread.startThread();
            }
        }
    }

    protected Long selectMaxId(JdbcTemplate jdbcTemplate, String idFiled, String tableName) {
        return jdbcTemplate.queryForObject("select max(" + idFiled + ") from " + tableName, Long.class);
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<Long> id,
                           boolean onlyCurrentIndex, Collection<String> onlyFieldNameSet,
                           ESAdapter esAdapter, ESSyncConfig config) {
        this.stop = false;
        if (id == null || id.isEmpty()) {
            return 0;
        }
        int count = 0;
        String pk = config.getEsMapping().getPk();
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        for (Long i : id) {
            if (stop) {
                break;
            }
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1, tableName, pk, config, null);
            esAdapter.sync(dmlList, false, false, onlyCurrentIndex, 1, onlyFieldNameSet, null);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(AbstractMessageService messageService, List<SyncRunnable> runnableList,
                            Date startTime, long dmlSize,
                            Set<String> onlyFieldNameSet,
                            ESAdapter adapter, ESSyncConfig config, boolean onlyCurrentIndex, String sqlWhere,
                            boolean insertIgnore) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n insertIgnore = " + insertIgnore
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 限制条件 = " + sqlWhere
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 明细 = " + runnableList.stream().map(SyncRunnable::toString).collect(Collectors.joining("\r\n,"));
        messageService.send(title, content);
    }

    protected void sendError(AbstractMessageService messageService, Throwable throwable,
                             SyncRunnable runnable, Long minOffset,
                             Set<String> onlyFieldNameSet,
                             ESAdapter adapter, ESSyncConfig config, boolean onlyCurrentIndex, String sqlWhere, boolean insertIgnore) {
        String title = "ES搜索全量刷数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n insertIgnore = " + insertIgnore
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 限制条件 = " + sqlWhere
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n threadIndex = " + runnable.getThreadIndex()
                + ",\n\n minOffset = " + minOffset
                + ",\n\n Runnable = " + runnable
                + ",\n\n 异常 = " + throwable
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    protected void sendTrimDone(AbstractMessageService messageService, Date startTime,
                                long dmlSize, long deleteSize, List<String> deleteIdList,
                                ESAdapter adapter, ESSyncConfig config) {
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

    protected void sendTrimError(AbstractMessageService messageService, Throwable throwable,
                                 Date timestamp, long dmlSize, long deleteSize, List<String> deleteIdList,
                                 ESAdapter adapter, ESSyncConfig config, String lastId) {
        String title = "ES搜索全量校验Trim数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 使用实现 = " + adapter.getConfiguration().getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 最近的ID = " + lastId
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
                                 ESAdapter adapter, ESSyncConfig config, String lastId) {
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
                + ",\n\n 最近的ID = " + lastId
                + ",\n\n 异常 = " + throwable
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID = " + updateIdList
                + ",\n\n 删除ID = " + deleteIdList
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
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

    protected List<Dml> filter(List<Dml> dmlList, DefaultESTemplate esTemplate, ESSyncConfig.ESMapping esMapping) {
        if (dmlList.isEmpty()) {
            return dmlList;
        }
        List<Long> idList = dmlList.stream().map(this::getDmlId).flatMap(Collection::stream).collect(Collectors.toList());
        Set<String> esIds = esTemplate.searchByIds(esMapping, idList);
        List<Dml> result = new ArrayList<>(dmlList.size());
        for (Dml dml : dmlList) {
            List<Long> dmlIdList = getDmlId(dml);
            if (dmlIdList.stream().anyMatch(e -> esIds.contains(String.valueOf(e)))) {
                continue;
            }
            result.add(dml);
        }
        return result;
    }

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Long minId, int limit, String tableName, String idColumnName, ESSyncConfig config, String where) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName, where);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Collections.singletonList(row), Collections.singletonList(idColumnName), tableName, catalog, new String[]{config.getDestination()}));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, Long minId, int limit, String tableName, String idColumnName, String where) {
        String sql;
        if (limit == 1) {
            sql = "select * from " + tableName + " " + tableName + " where " + tableName + "." + idColumnName + " = ?";
        } else {
            sql = "select * from " + tableName + " " + tableName + " where " + tableName + "." + idColumnName + " > ?";
        }
        boolean whereAppend = Util.isNotBlank(where);
        if (whereAppend) {
            sql += " and (" + where + ")";
        }
        sql += " order by " + tableName + "." + idColumnName + " limit ?";
        if (whereAppend) {
            log.info("selectList sql = {}", sql);
        }
        return jdbcTemplate.queryForList(sql, minId, limit);
    }

    protected Long getDmlListMaxId(List<Dml> list) {
        long maxId = Long.MIN_VALUE;
        for (Dml dml : list) {
            List<Long> dmlIdList = getDmlId(dml);
            for (Long id : dmlIdList) {
                if (id > maxId) {
                    maxId = id;
                }
            }
        }
        return maxId == Long.MIN_VALUE ? null : maxId;
    }

    protected List<Long> getDmlId(Dml dml) {
        List<String> pkNames = dml.getPkNames();
        if (pkNames.size() != 1) {
            throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
        }
        String pkName = pkNames.iterator().next();
        List<Map<String, Object>> data = dml.getData();
        List<Long> ids = new ArrayList<>(data.size());
        for (Map<String, Object> row : data) {
            Object o = row.get(pkName);
            if (o == null) {
                continue;
            }
            long id;
            if (o instanceof Long) {
                id = (Long) o;
            } else {
                id = Long.parseLong(o.toString());
            }
            ids.add(id);
        }
        return ids;
    }

    public String getName() {
        return name;
    }

    public static abstract class SyncRunnable implements Runnable {
        private static final List<SyncRunnable> RUNNABLE_LIST = Collections.synchronizedList(new ArrayList<>());
        protected final int threadIndex;
        private final long maxId;
        private final int threads;
        private final long offset;
        private final long endOffset;
        private final long offsetStart;
        private final IntESETLService service;
        private final String name;
        private final Set<String> onlyFieldNameSet;
        private final ESAdapter adapter;
        private final ESSyncConfig config;
        private final boolean onlyCurrentIndex;
        private final String sqlWhere;
        private final boolean insertIgnore;
        protected long currOffset;
        private boolean done;

        public SyncRunnable(String name, IntESETLService service,
                            int threadIndex, long offsetStart, long maxId, int threads,
                            Set<String> onlyFieldNameSet,
                            ESAdapter adapter,
                            ESSyncConfig config, boolean onlyCurrentIndex,
                            String sqlWhere,
                            boolean insertIgnore) {
            this.name = name;
            this.insertIgnore = insertIgnore;
            this.onlyCurrentIndex = onlyCurrentIndex;
            this.onlyFieldNameSet = onlyFieldNameSet;
            this.adapter = adapter;
            this.config = config;
            this.service = service;
            this.threadIndex = threadIndex;
            this.maxId = maxId;
            this.threads = threads;
            this.offsetStart = offsetStart;
            long allocation = ((maxId + 1 - offsetStart) / threads);
            this.offset = offsetStart + (threadIndex * allocation);
            this.endOffset = threadIndex + 1 == threads ? offset + allocation * 3 : offset + allocation;
            this.currOffset = offset;
            this.sqlWhere = sqlWhere;
            RUNNABLE_LIST.add(this);
        }

        public static Long minOffset(List<SyncRunnable> list) {
            Long min = null;
            for (SyncRunnable runnable : list) {
                if (!runnable.done) {
                    if (min == null) {
                        min = runnable.currOffset;
                    } else {
                        min = Math.min(min, runnable.currOffset);
                    }
                }
            }
            return min;
        }

        public long getCurrOffset() {
            return currOffset;
        }

        public String getName() {
            return name;
        }

        public boolean isDone() {
            return done;
        }

        @Override
        public void run() {
            int i = 0;
            try {
                for (currOffset = offset, i = 0; currOffset < endOffset; i++) {
                    long ts = System.currentTimeMillis();
                    long cOffsetbefore = currOffset;
                    currOffset = run0(currOffset);
                    log.info("all sync threadIndex {}/{}, offset = {}-{}, i ={}, remain = {}, cost = {}ms, maxId = {}",
                            threadIndex, threads, cOffsetbefore, currOffset, i,
                            endOffset - currOffset, System.currentTimeMillis() - ts, maxId);
                    if (currOffset == Long.MAX_VALUE) {
                        break;
                    }
                    if (cOffsetbefore == currOffset && cOffsetbefore == maxId) {
                        break;
                    }
                }
            } catch (Exception e) {
                Long minOffset = SyncRunnable.minOffset(SyncRunnable.RUNNABLE_LIST);
                service.sendError(service.startupServer.getMessageService(), e, this, minOffset, onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere, insertIgnore);
                throw e;
            } finally {
                done = true;
                log.info("all sync done threadIndex {}/{}, offset = {}, i ={}, maxId = {}, info ={} ",
                        threadIndex, threads, currOffset, i, maxId, this);
                done();
            }
        }

        public void done() {

        }

        public abstract long run0(long offset);

        public int getThreadIndex() {
            return threadIndex;
        }

        public long getMaxId() {
            return maxId;
        }

        public int getThreads() {
            return threads;
        }

        public long getOffset() {
            return offset;
        }

        public long getEndOffset() {
            return endOffset;
        }

        public long getOffsetStart() {
            return offsetStart;
        }

        @Override
        public String toString() {
            return threadIndex + ":" + threads + "{" +
                    "start=" + offset +
                    ", end=" + currOffset +
                    ", max=" + endOffset +
                    '}';
        }
    }
}
