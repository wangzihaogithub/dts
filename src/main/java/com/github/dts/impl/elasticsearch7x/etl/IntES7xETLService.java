package com.github.dts.impl.elasticsearch7x.etl;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.impl.elasticsearch7x.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
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
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncById?id=1,2">http://localhost:8080/es7x/myxxx/syncById?id=1,2</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncAll">http://localhost:8080/es7x/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/stop">http://localhost:8080/es7x/myxxx/stop</a>"
 * </pre>
 */
public class IntES7xETLService {
    private static final Logger log = LoggerFactory.getLogger(IntES7xETLService.class);
    protected final StartupServer startupServer;
    private final ExecutorService executorService;
    private final String name;
    private boolean stop = false;

    public IntES7xETLService(String name, StartupServer startupServer) {
        this.name = name;
        this.startupServer = startupServer;
        this.executorService = Util.newFixedThreadPool(1000, 5000L,
                name, true);
    }

    public List<SyncRunnable> syncAll(String esIndexName) {
        return syncAll(esIndexName,
                50, 0, null, 500,
                true, false, true, 100, null);
    }

    public Object syncById(Long[] id,
                           String esIndexName) {
        return syncById(id, esIndexName, true, null);
    }

    public int updateEsDiff(String esIndexName) {
        return updateEsDiff(esIndexName, 500, null, 500);
    }

    public int deleteEsTrim(String esIndexName) {
        return deleteEsTrim(esIndexName, 500, 1000);
    }

    public int deleteEsTrim(String esIndexName,
                            int offsetAdd,
                            int maxSendMessageDeleteIdSize) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        int r = 0;
        for (ES7xAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return r;
        }

        executorService.execute(() -> {
            AbstractMessageService messageService = startupServer.getMessageService();
            for (ES7xAdapter adapter : adapterList) {
                long hitListSize = 0;
                long deleteSize = 0;
                List<String> deleteIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                ESSyncConfig lastConfig = null;
                try {
                    Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                    for (ESSyncConfig config : configMap.values()) {
                        lastConfig = config;
                        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
                        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                        String pk = esMapping.getPk();
                        String tableName = esMapping.getSchemaItem().getMainTable().getTableName();

                        ESTemplate esTemplate = adapter.getEsTemplate();

                        Object[] searchAfter = null;
                        do {
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
                                if (!dbIds.contains(id)) {
                                    esTemplate.delete(esMapping, id, null, null);
                                    deleteSize++;
                                    if (deleteIdList.size() < maxSendMessageDeleteIdSize) {
                                        deleteIdList.add(id);
                                    }
                                }
                            }
                            esTemplate.commit();
                            searchAfter = searchResponse.getLastSortValues();
                            log.info("deleteEsTrim searchAfter = {}", searchAfter);
                        } while (true);
                        sendTrimDone(messageService, timestamp, hitListSize, deleteSize, deleteIdList, adapter, config);
                    }
                } catch (Exception e) {
                    sendTrimError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, adapter, lastConfig);
                }
            }
        });
        return r;
    }

    public int updateEsDiff(String esIndexName,
                            int offsetAdd,
                            Set<String> diffFields,
                            int maxSendMessageSize) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        int r = 0;
        for (ES7xAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return r;
        }

        executorService.execute(() -> {
            AbstractMessageService messageService = startupServer.getMessageService();
            for (ES7xAdapter adapter : adapterList) {
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

                        ES7xTemplate esTemplate = adapter.getEsTemplate();

                        Object[] searchAfter = null;
                        do {
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
                                Map<String, Object> db = dbMap.get(id);
                                if (db != null) {
                                    if (!ESSyncUtil.equalsRowData(db, hit, diffFields, esMapping)) {
                                        updateSize++;
                                        if (updateIdList.size() < maxSendMessageSize) {
                                            updateIdList.add(id);
                                        }
                                        esTemplate.update(esMapping, id, hit, null);
                                    }
                                } else {
                                    esTemplate.delete(esMapping, id, null, null);
                                    deleteSize++;
                                    if (deleteIdList.size() < maxSendMessageSize) {
                                        deleteIdList.add(id);
                                    }
                                }
                            }
                            esTemplate.commit();
                            searchAfter = searchResponse.getLastSortValues();
                            log.info("updateEsDiff searchAfter = {}", searchAfter);
                        } while (true);
                        sendDiffDone(messageService, timestamp, hitListSize, deleteSize, deleteIdList, updateSize, updateIdList, diffFields, adapter, config);
                    }
                } catch (Exception e) {
                    sendDiffError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, updateSize, updateIdList, diffFields, adapter, lastConfig);
                }
            }
        });
        return r;
    }

    public int updateEsNestedDiff(String esIndexName) {
        return updateEsNestedDiff(esIndexName, null, null, 500, null, 1000);
    }

    public int updateEsNestedDiff(String esIndexName,
                                  Long startId,
                                  Long endId,
                                  int offsetAdd,
                                  Set<String> diffFields,
                                  int maxSendMessageSize) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        int r = 0;
        for (ES7xAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return r;
        }

        executorService.execute(() -> {
            String lastId = null;
            AbstractMessageService messageService = startupServer.getMessageService();
            for (ES7xAdapter adapter : adapterList) {
                long hitListSize = 0;
                long updateSize = 0;
                List<String> updateIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                ES7xTemplate esTemplate = adapter.getEsTemplate();

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
                        Object[] searchAfter = startId == null ? null : new Object[]{startId - 1L};
                        do {
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
                                Set<String> cols = SQL.convertToSql(objectField.getFullSql(true), Collections.emptyMap()).getArgsMap().keySet();
                                for (String col : cols) {
                                    select.add("`" + col + "`");
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
                                    SQL sql1 = SQL.convertToSql(objectField.getFullSql(true), map);
                                    EsJdbcTemplateSQL jdbcTemplateSQL = new EsJdbcTemplateSQL(sql1.getExprSql(), sql1.getArgs(), map, config.getDataSourceKey(),
                                            objectField.getSchemaItem().getGroupByIdColumns(), hit);
                                    list.add(jdbcTemplateSQL);
                                }
                                jdbcTemplateSQLList.put(diffField, list);
                            }

                            Map<String, Future<Map<String, List<Map<String, Object>>>>> fieldDbMap = new HashMap<>(jdbcTemplateSQLList.size());
                            if (jdbcTemplateSQLList.size() == 1) {
                                for (Map.Entry<String, List<EsJdbcTemplateSQL>> entry : jdbcTemplateSQLList.entrySet()) {
                                    List<MergeJdbcTemplateSQL<EsJdbcTemplateSQL>> mergeList = MergeJdbcTemplateSQL.merge(entry.getValue(), 1000);
                                    fieldDbMap.put(entry.getKey(), CompletableFuture.completedFuture(MergeJdbcTemplateSQL.toMap(mergeList, e -> e.getHit().getId())));
                                }
                            } else {
                                for (Map.Entry<String, List<EsJdbcTemplateSQL>> entry : jdbcTemplateSQLList.entrySet()) {
                                    fieldDbMap.put(entry.getKey(), executorService.submit(() -> {
                                        List<MergeJdbcTemplateSQL<EsJdbcTemplateSQL>> mergeList = MergeJdbcTemplateSQL.merge(entry.getValue(), 1000);
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
                            log.info("updateEsNestedDiff searchAfter = {}", searchAfter);
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
            }
        });
        return r;
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
            Set<String> onlyFieldNameSet) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }

        this.stop = false;
        List<SyncRunnable> runnableList = new ArrayList<>();
        for (ES7xAdapter adapter : adapterList) {
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
            setSuspendEs7x(true, clientIdentity);

            AbstractMessageService messageService = startupServer.getMessageService();
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                String pk = config.getEsMapping().getPk();
                String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();

                Long maxId = offsetEnd == null || offsetEnd == Long.MAX_VALUE ?
                        selectMaxId(jdbcTemplate, pk, tableName) : offsetEnd;

                Date timestamp = new Timestamp(System.currentTimeMillis());
                AtomicInteger done = new AtomicInteger(0);
                AtomicLong dmlSize = new AtomicLong(0);
                for (int i = 0; i < threads; i++) {
                    runnableList.add(new SyncRunnable(name, this,
                            i, offsetStart, maxId, threads, onlyFieldNameSet, adapter, config, onlyCurrentIndex) {
                        @Override
                        public long run0(long offset) {
                            if (stop) {
                                return Long.MAX_VALUE;
                            }
                            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, offset, offsetAdd, tableName, pk, config);

                            if (!append) {
                                Long dmlListMaxId = getDmlListMaxId(dmlList);
                                adapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, offset, dmlListMaxId, offsetAdd);
                            }
                            if (!dmlList.isEmpty()) {
                                adapter.sync(dmlList, false, true, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet);
                            }
                            dmlSize.addAndGet(dmlList.size());
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.longValue(), SyncRunnable.minOffset(runnableList));
                            }
                            Long dmlListMaxId = getDmlListMaxId(dmlList);
                            return dmlListMaxId == null ? Long.MAX_VALUE : dmlListMaxId;
                        }

                        @Override
                        public void done() {
                            if (done.incrementAndGet() == threads) {
                                if (log.isInfoEnabled()) {
                                    log.info("syncAll done {}", this);
                                }
                                setSuspendEs7x(false, clientIdentity);
                                sendDone(messageService, runnableList, timestamp, dmlSize.longValue(), onlyFieldNameSet, adapter, config, onlyCurrentIndex);
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
                        Set<String> onlyFieldNameSet) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);

        int count = 0;
        for (ES7xAdapter adapter : adapterList) {
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

    public void stopSync() {
        stop = true;
    }

    public List discard(String clientIdentity) throws InterruptedException {
        List list = new ArrayList();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(clientIdentity)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    private void setSuspendEs7x(boolean suspend, String clientIdentity) {
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
                           ES7xAdapter esAdapter, ESSyncConfig config) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        int count = 0;
        String pk = config.getEsMapping().getPk();
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        for (Long i : id) {
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1, tableName, pk, config);
            esAdapter.sync(dmlList, false, true, onlyCurrentIndex, 1, onlyFieldNameSet);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(AbstractMessageService messageService, List<SyncRunnable> runnableList,
                            Date startTime, long dmlSize,
                            Set<String> onlyFieldNameSet,
                            ES7xAdapter adapter, ESSyncConfig config, boolean onlyCurrentIndex) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 明细 = " + runnableList.stream().map(SyncRunnable::toString).collect(Collectors.joining("\r\n,"));
        messageService.send(title, content);
    }

    protected void sendError(AbstractMessageService messageService, Throwable throwable,
                             SyncRunnable runnable, Long minOffset,
                             Set<String> onlyFieldNameSet,
                             ES7xAdapter adapter, ESSyncConfig config, boolean onlyCurrentIndex) {
        String title = "ES搜索全量刷数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
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
                                ES7xAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Trim数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 删除ID = " + deleteIdList;
        messageService.send(title, content);
    }

    protected void sendTrimError(AbstractMessageService messageService, Throwable throwable,
                                 Date timestamp, long dmlSize, long deleteSize, List<String> deleteIdList,
                                 ES7xAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Trim数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
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
                                ES7xAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Diff数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
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
                                 ES7xAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Diff数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
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

    protected void sendNestedDiffDone(AbstractMessageService messageService, Date startTime,
                                      long dmlSize,
                                      long updateSize, List<String> updateIdList,
                                      Set<String> diffFields,
                                      ES7xAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验嵌套Diff数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
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
                                       ES7xAdapter adapter, ESSyncConfig config,
                                       String lastId) {
        String title = "ES搜索全量校验嵌套Diff数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
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

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Long minId, int limit, String tableName, String idColumnName, ESSyncConfig config) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Arrays.asList(row), Arrays.asList(idColumnName), tableName, catalog, new String[]{config.getDestination()}));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, Long minId, int limit, String tableName, String idColumnName) {
        String sql;
        if (limit == 1) {
            sql = "select * from " + tableName + " where " + idColumnName + " = ? limit ?";
        } else {
            sql = "select * from " + tableName + " where " + idColumnName + " > ? limit ?";
        }
        return jdbcTemplate.queryForList(sql, minId, limit);
    }

    protected Long getDmlListMaxId(List<Dml> list) {
        long maxId = Long.MIN_VALUE;
        for (Dml dml : list) {
            List<String> pkNames = dml.getPkNames();
            if (pkNames.size() != 1) {
                throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
            }
            String pkName = pkNames.iterator().next();
            for (Map<String, Object> row : dml.getData()) {
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
                if (id > maxId) {
                    maxId = id;
                }
            }
        }
        return maxId == Long.MIN_VALUE ? null : maxId;
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
        private final IntES7xETLService service;
        private final String name;
        private final Set<String> onlyFieldNameSet;
        private final ES7xAdapter adapter;
        private final ESSyncConfig config;
        private final boolean onlyCurrentIndex;
        protected long currOffset;
        private boolean done;

        public SyncRunnable(String name, IntES7xETLService service,
                            int threadIndex, long offsetStart, long maxId, int threads,
                            Set<String> onlyFieldNameSet,
                            ES7xAdapter adapter,
                            ESSyncConfig config, boolean onlyCurrentIndex) {
            this.name = name;
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
                service.sendError(service.startupServer.getMessageService(), e, this, minOffset, onlyFieldNameSet, adapter, config, onlyCurrentIndex);
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
