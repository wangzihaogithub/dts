package com.github.dts.impl.elasticsearch.etl;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.elasticsearch.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
public class IntESETLService implements ESETLService {
    private static final Logger log = LoggerFactory.getLogger(IntESETLService.class);
    protected final StartupServer startupServer;
    private final ExecutorService executorService;
    private final String name;
    private final AtomicInteger taskId = new AtomicInteger();
    private volatile int stopTaskId = 0;
    private boolean sendMessage = true;
    private int maxChangeInfoListSize = Integer.MAX_VALUE;

    public IntESETLService(String name, StartupServer startupServer) {
        this(name, startupServer, Math.max(Runtime.getRuntime().availableProcessors() * 20, 50));
    }

    public IntESETLService(String name, StartupServer startupServer, int threads) {
        this.name = name;
        this.startupServer = startupServer;
        this.executorService = Util.newFixedThreadPool(threads, 5000L,
                name, true);
    }

    public void setMaxChangeInfoListSize(int maxChangeInfoListSize) {
        this.maxChangeInfoListSize = maxChangeInfoListSize;
    }

    public int getMaxChangeInfoListSize() {
        return maxChangeInfoListSize;
    }

    @Override
    public void stopSync() {
        this.stopTaskId = this.taskId.intValue();
    }

    public List<CompletableFuture<?>> checkAll(String esIndexName, List<String> adapterNames, int offsetAdd, int threads, String sqlWhere, String esQueryBodyJson) {
        List<SyncRunnable> l1 = syncAll(esIndexName, threads, null, null, offsetAdd,
                true, 100, null, adapterNames, sqlWhere, true, 50);
        List<CompletableFuture<Counter>> l2 = updateEsDiff(esIndexName, null, null, offsetAdd, threads, null, 50, adapterNames, esQueryBodyJson);
        List<CompletableFuture<Counter>> l3 = updateEsNestedDiff(esIndexName, null, null, offsetAdd, threads, null, 50, adapterNames, esQueryBodyJson);

        List<CompletableFuture<?>> futureList = new ArrayList<>();
        futureList.addAll(l1);
        futureList.addAll(l2);
        futureList.addAll(l3);
        return futureList;
    }

    public List<SyncRunnable> syncAll(String esIndexName) {
        return syncAll(esIndexName,
                10, null, null, 100,
                true, 100, null, null, null, false, 50);
    }

    public int syncById(Long[] id,
                        String esIndexName) {
        return syncById(id, esIndexName, true, null, null);
    }

    public List<CompletableFuture<Counter>> updateEsDiff(String esIndexName) {
        return updateEsDiff(esIndexName, null, null, 1000, 0, null, 50, null, null);
    }

    public List<CompletableFuture<Void>> deleteEsTrim(String esIndexName) {
        return deleteEsTrim(esIndexName, null, null, 1000, 50, null);
    }

    private boolean isStop(int taskId) {
        return taskId <= this.stopTaskId;
    }

    public List<CompletableFuture<Void>> deleteEsTrim(String esIndexName,
                                                      Long startId,
                                                      Long endId,
                                                      int offsetAdd,
                                                      int maxSendMessageDeleteIdSize,
                                                      List<String> adapterNames) {
        int taskId = this.taskId.incrementAndGet();
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }
        int r = 0;
        for (ESAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return new ArrayList<>();
        }

        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        AbstractMessageService messageService = startupServer.getMessageService();
        for (ESAdapter adapter : adapterList) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            futureList.add(future);
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

                        Object[] searchAfter = startId == null ? null : new Object[]{Math.max(startId - 1L, 0L)};
                        do {
                            if (isStop(taskId)) {
                                break;
                            }
                            if (endId != null
                                    && searchAfter != null && searchAfter.length > 0
                                    && searchAfter[0] != null
                                    && isEndSearchAfter(searchAfter[0], endId)) {
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
                        if (sendMessage) {
                            sendTrimDone(messageService, timestamp, hitListSize, deleteSize, deleteIdList, adapter, config);
                        }
                    }
                    future.complete(null);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                    sendTrimError(messageService, e, timestamp, hitListSize, deleteSize, deleteIdList, adapter, lastConfig, lastId);
                }
            });
        }
        return futureList;
    }

    /**
     * 避免id字段的es类型为keyword，数据库为int
     *
     * @param searchAfter id字段值
     * @param endId       最大范围
     * @return true=结束遍历searchAfter
     */
    private static boolean isEndSearchAfter(Object searchAfter, Long endId) {
        if (searchAfter instanceof String) {
            return ((String) searchAfter).compareTo(endId.toString()) >= 0;
        } else if (searchAfter instanceof Number) {
            return ((Number) searchAfter).longValue() >= endId;
        } else {
            return Long.parseLong(searchAfter.toString()) >= endId;
        }
    }

    public List<CompletableFuture<Counter>> updateEsDiff(String esIndexName,
                                                         Long startId,
                                                         Long endId,
                                                         int offsetAdd,
                                                         int threads,
                                                         Set<String> diffFields,
                                                         int maxSendMessageSize,
                                                         List<String> adapterNames,
                                                         String esQueryBodyJson) {
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }
        int r = 0;
        for (ESAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return new ArrayList<>();
        }
        int taskId = this.taskId.incrementAndGet();
        if (threads <= 0) {
            threads = Math.max(Runtime.getRuntime().availableProcessors(), 1) * 2;
        }
        List<CompletableFuture<Counter>> resultFutureList = new ArrayList<>();
        AbstractMessageService messageService = startupServer.getMessageService();
        for (ESAdapter adapter : adapterList) {
            DefaultESTemplate esTemplate = adapter.getEsTemplate();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ESSyncConfig[] lastConfig = new ESSyncConfig[1];
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                lastConfig[0] = config;
                Long itemStartId = startId;
                Long itemEndId = endId;
                if (itemStartId == null) {
                    Long minValue = esTemplate.searchMinValue(config.getEsMapping().get_index(), config.getEsMapping().getPk(), Long.class);
                    itemStartId = minValue == null ? 0 : minValue;
                }
                if (itemEndId == null) {
                    Long maxValue = esTemplate.searchMaxValue(config.getEsMapping().get_index(), config.getEsMapping().getPk(), Long.class);
                    itemEndId = maxValue == null ? 1 : maxValue;
                }
                List<Util.Range> rangeList = Util.splitRange(itemStartId, itemEndId, threads);
                Counter[] counterList = new Counter[rangeList.size()];
                CompletableFuture<Void>[] futureList = new CompletableFuture[rangeList.size()];
                for (int i = 0, size = rangeList.size(); i < size; i++) {
                    Util.Range range = rangeList.get(i);
                    Counter counter = new Counter();
                    counterList[i] = counter;
                    futureList[i] = newUpdateEsDiffRunnable(counter, esTemplate, config, range.getStart(), range.isLast() ? null : range.getEnd(), offsetAdd, diffFields, maxSendMessageSize, esQueryBodyJson, taskId);
                }
                CompletableFuture<Counter> future = CompletableFuture.allOf(futureList).thenApply(unused -> Counter.merge(counterList, maxSendMessageSize, maxChangeInfoListSize));
                resultFutureList.add(future);
                future.thenAccept(merge -> {
                            if (sendMessage) {
                                sendDiffDone(messageService, timestamp, merge.hitListSize, merge.deleteSize, merge.deleteIdList, merge.updateSize, merge.updateIdList, merge.allRowChangeMap, diffFields, adapter, config);
                            }
                        })
                        .exceptionally(throwable -> {
                            log.error("updateEsDiff error {}", throwable.toString(), throwable);
                            Counter merge = Counter.merge(counterList, maxSendMessageSize, maxChangeInfoListSize);
                            sendDiffError(messageService, throwable, timestamp, merge.hitListSize, merge.deleteSize, merge.deleteIdList, merge.updateSize, merge.updateIdList, merge.allRowChangeMap, diffFields, adapter, lastConfig[0], merge.lastId);
                            return null;
                        });
            }
        }
        return resultFutureList;
    }

    public void setSendMessage(boolean sendMessage) {
        this.sendMessage = sendMessage;
    }

    public boolean isSendMessage() {
        return sendMessage;
    }

    public static class Counter {
        private String lastId = null;
        private long hitListSize = 0;
        private long deleteSize = 0;
        private long updateSize = 0;
        private final ArrayList<String> deleteIdList = new ArrayList<>();
        private final ArrayList<String> updateIdList = new ArrayList<>();
        private final ArrayList<String> changeInfoList = new ArrayList<>();
        private final Map<String, AtomicInteger> allRowChangeMap = new LinkedHashMap<>();

        public static Counter merge(Counter[] counters, int maxSendMessageSize, int maxChangeInfoListSize) {
            Counter merge = new Counter();
            if (counters.length == 0) {
                return merge;
            }
            if (counters.length == 1) {
                return counters[0];
            }
            merge.lastId = counters[counters.length - 1].lastId;
            ArrayList<String> deleteIdList = new ArrayList<>();
            ArrayList<String> updateIdList = new ArrayList<>();
            ArrayList<String> changeInfoList = new ArrayList<>();
            for (Counter counter : counters) {
                merge.hitListSize += counter.hitListSize;
                merge.deleteSize += counter.deleteSize;
                merge.updateSize += counter.updateSize;
                if (deleteIdList.size() < maxSendMessageSize) {
                    deleteIdList.addAll(counter.deleteIdList);
                }
                if (updateIdList.size() < maxSendMessageSize) {
                    updateIdList.addAll(counter.updateIdList);
                }
                if (changeInfoList.size() < Math.min(maxChangeInfoListSize, maxSendMessageSize)) {
                    changeInfoList.addAll(counter.changeInfoList);
                }
                counter.allRowChangeMap.forEach((k, v) -> merge.allRowChangeMap.computeIfAbsent(k, u -> new AtomicInteger()).addAndGet(v.get()));
            }
            merge.deleteIdList.addAll(deleteIdList.subList(0, Math.min(deleteIdList.size(), maxSendMessageSize)));
            merge.updateIdList.addAll(updateIdList.subList(0, Math.min(updateIdList.size(), maxSendMessageSize)));
            merge.changeInfoList.addAll(changeInfoList.subList(0, Math.min(changeInfoList.size(), Math.min(maxChangeInfoListSize, maxSendMessageSize))));
            return merge;
        }

        public String getLastId() {
            return lastId;
        }

        public long getHitListSize() {
            return hitListSize;
        }

        public long getDeleteSize() {
            return deleteSize;
        }

        public long getUpdateSize() {
            return updateSize;
        }

        public ArrayList<String> getDeleteIdList() {
            return deleteIdList;
        }

        public ArrayList<String> getUpdateIdList() {
            return updateIdList;
        }

        public Map<String, AtomicInteger> getAllRowChangeMap() {
            return allRowChangeMap;
        }
    }

    private CompletableFuture<Void> newUpdateEsDiffRunnable(Counter counter, DefaultESTemplate esTemplate, ESSyncConfig config,
                                                            Long startId, Long endId, int offsetAdd,
                                                            Set<String> diffFields, int maxSendMessageSize, String esQueryBodyJson, int taskId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executorService.execute(() -> {
            try {
                ESSyncConfig.ESMapping esMapping = config.getEsMapping();
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String pkFieldName = esMapping.getSchemaItem().getIdField().getFieldName();
                String pkFieldExpr = esMapping.getSchemaItem().getIdField().getOwnerAndColumnName();
                String[] selectFields = esMapping.getSchemaItem().getSelectFields().keySet().stream().filter(e -> !esMapping.isLlmVector(e)).toArray(String[]::new);

                Object[] searchAfter = startId == null ? null : new Object[]{Math.max(startId - 1L, 0L)};
                do {
                    if (isStop(taskId)) {
                        break;
                    }
                    if (endId != null
                            && searchAfter != null && searchAfter.length > 0
                            && searchAfter[0] != null
                            && isEndSearchAfter(searchAfter[0], endId)) {
                        break;
                    }
                    ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfter(esMapping, selectFields, null, searchAfter, offsetAdd, esQueryBodyJson);
                    List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                    if (hitList.isEmpty()) {
                        break;
                    }
                    counter.hitListSize += hitList.size();

                    String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining(","));
                    String sql = String.format("%s where %s in (%s) group by %s", esMapping.getSql(), pkFieldExpr, ids, pkFieldExpr);
                    Map<String, Map<String, Object>> dbMap = jdbcTemplate.queryForList(sql).stream()
                            .collect(Collectors.toMap(e -> String.valueOf(e.get(pkFieldName)), Function.identity()));
                    for (ESTemplate.Hit hit : hitList) {
                        String id = hit.getId();
                        counter.lastId = id;
                        Map<String, Object> db = dbMap.get(id);
                        if (db != null) {
                            Collection<String> rowChangeList = ESSyncUtil.getRowChangeList(db, hit, diffFields, esMapping);
                            if (!rowChangeList.isEmpty()) {
                                counter.updateSize++;
                                rowChangeList.forEach(e -> counter.allRowChangeMap.computeIfAbsent(e, __ -> new AtomicInteger()).incrementAndGet());
                                if (counter.updateIdList.size() < maxSendMessageSize) {
                                    counter.updateIdList.add(id);
                                }
                                Map<String, Object> mysqlValue = EsGetterUtil.copyRowChangeMap(db, rowChangeList);
                                esTemplate.update(esMapping, id, mysqlValue, null);//updateEsDiff
                            }
                        } else {
                            esTemplate.delete(esMapping, id, null);
                            counter.deleteSize++;
                            if (counter.deleteIdList.size() < maxSendMessageSize) {
                                counter.deleteIdList.add(id);
                            }
                        }
                    }
                    esTemplate.commit();
                    searchAfter = searchResponse.getLastSortValues();
                    log.info("updateEsDiff hits={}, searchAfter = {}", counter.hitListSize, searchAfter);
                } while (!Thread.currentThread().isInterrupted());
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    public List<CompletableFuture<Counter>> updateEsNestedDiff(String esIndexName) {
        return updateEsNestedDiff(esIndexName, null, null, 1000, 0, null, 50, null, null);
    }

    public List<CompletableFuture<Counter>> updateEsNestedDiff(String esIndexName,
                                                               Long startId,
                                                               Long endId,
                                                               int offsetAdd,
                                                               int threads,
                                                               Set<String> diffFields,
                                                               int maxSendMessageSize,
                                                               List<String> adapterNames,
                                                               String esQueryBodyJson) {
        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }
        int r = 0;
        for (ESAdapter adapter : adapterList) {
            r += adapter.getEsSyncConfigByIndex(esIndexName).size();
        }
        if (r == 0) {
            return new ArrayList<>();
        }
        if (threads <= 0) {
            threads = Math.max(Runtime.getRuntime().availableProcessors(), 1) * 2;
        }
        int taskId = this.taskId.incrementAndGet();
        int maxIdInCount = 1000;
        AbstractMessageService messageService = startupServer.getMessageService();
        List<CompletableFuture<Counter>> resultFutureList = new ArrayList<>();
        for (ESAdapter adapter : adapterList) {
            DefaultESTemplate esTemplate = adapter.getEsTemplate();

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            ESSyncConfig[] lastConfig = new ESSyncConfig[1];
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                lastConfig[0] = config;
                Long itemStartId = startId;
                Long itemEndId = endId;
                if (itemStartId == null) {
                    Long minValue = esTemplate.searchMinValue(config.getEsMapping().get_index(), config.getEsMapping().getPk(), Long.class);
                    itemStartId = minValue == null ? 0 : minValue;
                }
                if (itemEndId == null) {
                    Long maxValue = esTemplate.searchMaxValue(config.getEsMapping().get_index(), config.getEsMapping().getPk(), Long.class);
                    itemEndId = maxValue == null ? 1 : maxValue;
                }
                Set<String> diffFieldsFinal;
                if (diffFields == null || diffFields.isEmpty()) {
                    diffFieldsFinal = config.getEsMapping().getObjFields().entrySet().stream().filter(e -> e.getValue().isSqlType()).map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedHashSet::new));
                } else {
                    diffFieldsFinal = diffFields;
                }
                List<Util.Range> rangeList = Util.splitRange(itemStartId, itemEndId, threads);
                Counter[] counterList = new Counter[rangeList.size()];
                CompletableFuture<Void>[] futureList = new CompletableFuture[rangeList.size()];
                for (int i = 0, size = rangeList.size(); i < size; i++) {
                    Util.Range range = rangeList.get(i);
                    Counter counter = new Counter();
                    counterList[i] = counter;
                    futureList[i] = newUpdateEsNestedDiffRunnable(counter, esTemplate, config, range.getStart(), range.isLast() ? null : range.getEnd(), offsetAdd, diffFieldsFinal, maxSendMessageSize, maxIdInCount, esQueryBodyJson, taskId);
                }
                CompletableFuture<Counter> future = CompletableFuture.allOf(futureList).thenApply(unused -> Counter.merge(counterList, maxSendMessageSize, maxChangeInfoListSize));
                resultFutureList.add(future);
                future.thenAccept(merge -> {
                            if (sendMessage) {
                                sendNestedDiffDone(messageService, timestamp, merge.hitListSize, merge.updateSize, merge.updateIdList, merge.changeInfoList, merge.allRowChangeMap, diffFieldsFinal, adapter, config);
                            }
                        })
                        .exceptionally(throwable -> {
                            log.error("updateEsNestedDiff error {}", throwable.toString(), throwable);
                            Counter merge = Counter.merge(counterList, maxSendMessageSize, maxChangeInfoListSize);
                            sendNestedDiffError(messageService, throwable, timestamp, merge.hitListSize, merge.updateSize, merge.updateIdList, merge.allRowChangeMap, diffFieldsFinal, adapter, lastConfig[0], merge.lastId);
                            return null;
                        });
            }
        }
        return resultFutureList;
    }

    private CompletableFuture<Void> newUpdateEsNestedDiffRunnable(Counter counter, DefaultESTemplate esTemplate, ESSyncConfig config,
                                                                  Long startId, Long endId, int offsetAdd,
                                                                  Set<String> diffFields, int maxSendMessageSize,
                                                                  int maxIdInCount,
                                                                  String esQueryBodyJson,
                                                                  int taskId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executorService.execute(() -> {
            try {
                JsonUtil.ObjectWriter objectWriter = JsonUtil.objectWriter();
                ESSyncConfig.ESMapping esMapping = config.getEsMapping();

                String pkFieldName = esMapping.getSchemaItem().getIdField().getFieldName();
                String pkFieldExpr = esMapping.getSchemaItem().getIdField().getOwnerAndColumnName();
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());

                int uncommit = 0;
                Object[] searchAfter = startId == null ? null : new Object[]{Math.max(startId - 1L, 0L)};
                do {
                    if (isStop(taskId)) {
                        break;
                    }
                    if (endId != null
                            && searchAfter != null && searchAfter.length > 0
                            && searchAfter[0] != null
                            && isEndSearchAfter(searchAfter[0], endId)) {
                        break;
                    }
                    ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfter(esMapping, diffFields.toArray(new String[0]), null, searchAfter, offsetAdd, esQueryBodyJson);
                    List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                    if (hitList.isEmpty()) {
                        break;
                    }
                    counter.hitListSize += hitList.size();
                    Set<String> select = new LinkedHashSet<>();
                    for (String diffField : diffFields) {
                        ESSyncConfig.ObjectField objectField = esMapping.getObjectField(null, diffField);
                        if (objectField == null || !objectField.isSqlType()) {
                            continue;
                        }
                        Set<String> cols = SQL.convertToSql(objectField.getParamSql().getFullSql(true), Collections.emptyMap()).getArgsMap().keySet();
                        for (String col : cols) {
                            select.add("`" + col + "`");
                        }
                    }
                    select.add("`" + pkFieldName + "`");
                    String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining(","));
                    String sql = String.format("select %s from %s where %s in (%s) group by %s",
                            String.join(",", select),
                            esMapping.getSchemaItem().getMainTable().getTableName(), pkFieldName, ids, pkFieldName);
                    Map<String, Map<String, Object>> db1Map = jdbcTemplate.queryForList(sql).stream()
                            .collect(Collectors.toMap(e -> String.valueOf(e.get(pkFieldName)), Function.identity()));

                    Map<String, List<EsJdbcTemplateSQL>> jdbcTemplateSQLList = new HashMap<>();
                    for (String diffField : diffFields) {
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
                            counter.lastId = id;
                            Object esData = hit.get(field);
                            List<Map<String, Object>> mysqlData = dbMap.get(id);
                            Collection<String> rowChangeList = ESSyncUtil.getNestedRowChangeList(mysqlData, esData, objectField);
                            if (rowChangeList.isEmpty()) {
                                continue;
                            }
                            Object mysqlValue = EsGetterUtil.getSqlObjectMysqlValue(objectField, mysqlData, esTemplate, esMapping);

                            rowChangeList.forEach(f -> counter.allRowChangeMap.computeIfAbsent(f, __ -> new AtomicInteger()).incrementAndGet());
                            counter.updateSize++;
                            if (counter.updateIdList.size() < maxSendMessageSize) {
                                counter.updateIdList.add(id);
                            }
                            if (counter.changeInfoList.size() < Math.min(maxChangeInfoListSize, maxSendMessageSize)) {
                                Map<String, Object> info = new LinkedHashMap<>(3);
                                info.put("id", id);
                                info.put("field", field);
                                info.put("mysqlValue", mysqlValue);
                                info.put("esValue", esData);
                                counter.changeInfoList.add(objectWriter.writeValueAsString(info));
                            }
                            Map<String, Object> objectMysql = Collections.singletonMap(objectField.getFieldName(), mysqlValue);
                            esTemplate.update(esMapping, id, objectMysql, null);//updateEsNestedDiff
                            if (++uncommit >= 35) {
                                uncommit = 0;
                                esTemplate.commit();
                            }
                        }
                    }
                    searchAfter = searchResponse.getLastSortValues();
                    log.info("updateEsNestedDiff hits={}, searchAfter = {}", counter.hitListSize, searchAfter);
                } while (!Thread.currentThread().isInterrupted());
                esTemplate.commit();
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    protected List<ESAdapter> getAdapterList(List<String> adapterNames) {
        List<ESAdapter> adapterList = startupServer.getAdapter(ESAdapter.class);
        if (adapterNames == null || adapterNames.isEmpty()) {
            return adapterList;
        } else {
            return adapterList.stream()
                    .filter(e -> adapterNames.contains(e.getName()))
                    .collect(Collectors.toList());
        }
    }

    public List<SyncRunnable> syncAll(
            String esIndexName,
            int threads,
            Number offsetStart,
            Number offsetEnd,
            int offsetAdd,
//            boolean append,
//            boolean discard,
            boolean onlyCurrentIndex,
            int joinUpdateSize,
            Set<String> onlyFieldNameSet,
            List<String> adapterNames,
            String sqlWhere,
            boolean insertIgnore,
            int maxSendMessageSize) {
        String trimWhere = ESSyncUtil.trimWhere(sqlWhere);

        List<ESAdapter> adapterList = getAdapterList(adapterNames);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }

        int taskId = this.taskId.incrementAndGet();

        if (threads <= 0) {
            threads = Math.max(Runtime.getRuntime().availableProcessors(), 1) * 2;
        }
        List<SyncRunnable> runnableList = new ArrayList<>();
        for (ESAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            if (configMap.isEmpty()) {
                continue;
            }
//            String clientIdentity = adapter.getClientIdentity();
//            if (discard) {
//                new Thread(() -> {
//                    try {
//                        discard(clientIdentity);
//                    } catch (InterruptedException e) {
//                        log.info("discard {}", e, e);
//                    }
//                }).start();
//            }
//            setSuspendEs(true, clientIdentity);

            AbstractMessageService messageService = startupServer.getMessageService();
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                String pk = config.getEsMapping().getPk();
                String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
                Long minId = offsetStart == null ?
                        selectMinId(jdbcTemplate, pk, tableName) : offsetStart.longValue();
                Long maxId = offsetEnd == null ?
                        selectMaxId(jdbcTemplate, pk, tableName) : offsetEnd.longValue();
                if (minId == null) {
                    minId = 0L;
                }
                if (maxId == null) {
                    maxId = 1L;
                }
                Date timestamp = new Timestamp(System.currentTimeMillis());
                AtomicLong dmlSize = new AtomicLong(0);
                AtomicLong updateSize = new AtomicLong(0);
                List<Util.Range> rangeList = Util.splitRange(minId, maxId, threads);
                AtomicInteger done = new AtomicInteger(rangeList.size());
                List<Long> updateIdList = Collections.synchronizedList(new ArrayList<>());
                for (Util.Range range : rangeList) {
                    runnableList.add(new SyncRunnable(runnableList, range, this,
                            onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere, insertIgnore) {
                        @Override
                        public long run0(long offset) {
                            if (isStop(taskId)) {
                                return offset;
                            }
                            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, offset, offsetAdd, tableName, pk, config, trimWhere);
                            Long dmlListMaxId = getDmlListMaxId(dmlList);
                            List<Dml> filterDmlList;
                            if (insertIgnore) {
                                filterDmlList = filter(dmlList, adapter.getEsTemplate(), config.getEsMapping());
                            } else {
                                filterDmlList = dmlList;
                            }
                            if (!filterDmlList.isEmpty()) {
                                adapter.sync(filterDmlList, false, false, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet, null);
                                updateSize.addAndGet(filterDmlList.size());
                                if (sendMessage && updateIdList.size() < maxSendMessageSize) {
                                    filterDmlList.stream().map(IntESETLService.this::getDmlId).flatMap(Collection::stream).forEach(updateIdList::add);
                                }
                            }
                            dmlSize.addAndGet(dmlList.size());
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.longValue(), SyncRunnable.minOffset(runnableList));
                            }
                            return dmlListMaxId == null ? offset : dmlListMaxId;
                        }

                        @Override
                        public void done() {
                            if (done.decrementAndGet() == 0) {
                                if (log.isInfoEnabled()) {
                                    log.info("syncAll done {}", this);
                                }
//                                if (configDone.decrementAndGet() == 0) {
//                                    setSuspendEs(false, clientIdentity);
//                                }
                                if (sendMessage) {
                                    sendDone(messageService, runnableList, timestamp, dmlSize.longValue(), updateSize.longValue(), onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere, insertIgnore, updateIdList.subList(0, Math.min(updateIdList.size(), maxSendMessageSize)));
                                }
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

        List<Long> idList = Arrays.asList(id);
        int taskId = this.taskId.incrementAndGet();
        int count = 0;
        for (ESAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                try {
                    count += syncById(jdbcTemplate, catalog, idList, onlyCurrentIndex, onlyFieldNameSet, adapter, config, taskId);
                } finally {
                    log.info("all sync end.  total = {} ", count);
                }
            }
        }
        return count;
    }

    public List<CompletableFuture<?>> rebuildIndex(ESAdapter esAdapter,
                                                   String esIndexName, String newIndexName,
                                                   int offsetAdd, int threads, String sqlWhere) throws IOException {
        EsActionResponse esActionResponse = esAdapter.getEsTemplate().indexAliasesTo(esIndexName, newIndexName);
        if (!esActionResponse.isSuccess()) {
            Map<String, Object> responseBody = esActionResponse.getResponseBody();
            throw new IOException("rebuildIndex indexAliasesTo fail!" + responseBody);
        }

        List<IntESETLService.SyncRunnable> l1 = syncAll(esIndexName, threads, null, null, offsetAdd,
                true, 100, null, Collections.singletonList(esAdapter.getName()),
                sqlWhere, false, 20);
        return new ArrayList<>(l1);
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

    protected Long selectMinId(JdbcTemplate jdbcTemplate, String idFiled, String tableName) {
        return jdbcTemplate.queryForObject("select min(" + idFiled + ") from " + tableName, Long.class);
    }

    protected Long selectMaxId(JdbcTemplate jdbcTemplate, String idFiled, String tableName) {
        return jdbcTemplate.queryForObject("select max(" + idFiled + ") from " + tableName, Long.class);
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<Long> id,
                           boolean onlyCurrentIndex, Collection<String> onlyFieldNameSet,
                           ESAdapter esAdapter, ESSyncConfig config, int taskId) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        int count = 0;
        String pk = config.getEsMapping().getPk();
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        for (Long i : id) {
            if (isStop(taskId)) {
                break;
            }
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1, tableName, pk, config, null);
            esAdapter.sync(dmlList, false, false, onlyCurrentIndex, 1, onlyFieldNameSet, null);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(AbstractMessageService messageService, List<SyncRunnable> runnableList,
                            Date startTime, long dmlSize, long updateSize,
                            Set<String> onlyFieldNameSet,
                            ESAdapter adapter, ESSyncConfig config, boolean onlyCurrentIndex, String sqlWhere,
                            boolean insertIgnore,
                            List<Long> updateIdList) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 方式 = syncAll"
                + ",\n\n 使用实现 = " + adapter.getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n insertIgnore = " + insertIgnore
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 限制条件 = " + sqlWhere
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID(预览) = " + updateIdList
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
                + ",\n\n 方式 = syncAll"
                + ",\n\n 使用实现 = " + adapter.getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n insertIgnore = " + insertIgnore
                + ",\n\n 是否需要更新关联索引 = " + !onlyCurrentIndex
                + ",\n\n 限制条件 = " + sqlWhere
                + ",\n\n 影响字段 = " + (onlyFieldNameSet == null ? "全部" : onlyFieldNameSet)
                + ",\n\n threadIndex = " + runnable.range.getIndex()
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
                + ",\n\n 方式 = deleteEsTrim"
                + ",\n\n 使用实现 = " + adapter.getName()
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
                + ",\n\n 方式 = deleteEsTrim"
                + ",\n\n 使用实现 = " + adapter.getName()
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
                                Map<String, AtomicInteger> allRowChangeMap,
                                Set<String> diffFields,
                                ESAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验Diff数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 方式 = updateEsDiff"
                + ",\n\n 使用实现 = " + adapter.getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 比较字段(不含nested) = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 影响字段数 = " + allRowChangeMap
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID(预览) = " + updateIdList
                + ",\n\n 删除ID = " + deleteIdList;
        messageService.send(title, content);
    }

    protected void sendDiffError(AbstractMessageService messageService, Throwable throwable,
                                 Date timestamp, long dmlSize, long deleteSize, List<String> deleteIdList,
                                 long updateSize, List<String> updateIdList,
                                 Map<String, AtomicInteger> allRowChangeMap,
                                 Set<String> diffFields,
                                 ESAdapter adapter, ESSyncConfig config, String lastId) {
        String title = "ES搜索全量校验Diff数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 方式 = updateEsDiff"
                + ",\n\n 使用实现 = " + adapter.getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 比较字段(不含nested) = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 影响字段数 = " + allRowChangeMap
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 最近的ID = " + lastId
                + ",\n\n 异常 = " + throwable
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID(预览) = " + updateIdList
                + ",\n\n 删除ID = " + deleteIdList
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    protected void sendNestedDiffDone(AbstractMessageService messageService, Date startTime,
                                      long dmlSize,
                                      long updateSize, List<String> updateIdList, List<String> changeInfoList,
                                      Map<String, AtomicInteger> allRowChangeMap,
                                      Set<String> diffFields,
                                      ESAdapter adapter, ESSyncConfig config) {
        String title = "ES搜索全量校验嵌套Diff数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 方式 = updateEsNestedDiff"
                + ",\n\n 使用实现 = " + adapter.getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 比较nested字段 = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 影响字段数 = " + allRowChangeMap
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新内容(预览) = " + changeInfoList
                + ",\n\n 更新ID(预览) = " + updateIdList;
        messageService.send(title, content);
    }

    protected void sendNestedDiffError(AbstractMessageService messageService, Throwable throwable,
                                       Date timestamp, long dmlSize,
                                       long updateSize, List<String> updateIdList,
                                       Map<String, AtomicInteger> allRowChangeMap,
                                       Set<String> diffFields,
                                       ESAdapter adapter, ESSyncConfig config,
                                       String lastId) {
        String title = "ES搜索全量校验嵌套Diff数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getName()
                + ",\n\n 方式 = updateEsNestedDiff"
                + ",\n\n 使用实现 = " + adapter.getName()
                + ",\n\n 索引 = " + config.getEsMapping().get_index()
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 比较nested字段 = " + (diffFields == null ? "全部" : diffFields)
                + ",\n\n 影响字段数 = " + allRowChangeMap
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 最近的ID = " + lastId
                + ",\n\n 异常 = " + throwable
                + ",\n\n 更新条数 = " + updateSize
                + ",\n\n 更新ID(预览) = " + updateIdList
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
            if (ESTemplate.isEmptyPk(o)) {
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

    public static abstract class SyncRunnable extends CompletableFuture<Void> implements Runnable {
        private final List<SyncRunnable> runnableList;
        private final IntESETLService service;
        private final Set<String> onlyFieldNameSet;
        private final ESAdapter adapter;
        private final ESSyncConfig config;
        private final boolean onlyCurrentIndex;
        private final String sqlWhere;
        private final boolean insertIgnore;
        private final Util.Range range;
        private long currOffset;

        public SyncRunnable(List<SyncRunnable> runnableList, Util.Range range,
                            IntESETLService service,
                            Set<String> onlyFieldNameSet,
                            ESAdapter adapter,
                            ESSyncConfig config, boolean onlyCurrentIndex,
                            String sqlWhere,
                            boolean insertIgnore) {
            this.runnableList = runnableList;
            this.insertIgnore = insertIgnore;
            this.onlyCurrentIndex = onlyCurrentIndex;
            this.onlyFieldNameSet = onlyFieldNameSet;
            this.adapter = adapter;
            this.config = config;
            this.service = service;
            this.range = range;
            this.sqlWhere = sqlWhere;
        }

        static Long minOffset(List<SyncRunnable> list) {
            Long min = null;
            for (SyncRunnable runnable : list) {
                if (min == null) {
                    min = runnable.currOffset;
                } else {
                    min = Math.min(min, runnable.currOffset);
                }
            }
            return min;
        }

        public long getCurrOffset() {
            return currOffset;
        }

        @Override
        public void run() {
            int i = 0;
            try {
                for (currOffset = range.getStart(), i = 0; range.isLast() || currOffset < range.getEnd(); i++) {
                    long ts = System.currentTimeMillis();
                    long beforeOffset = currOffset;
                    currOffset = run0(currOffset);
                    log.info("all sync threadIndex {}/{}, offset = {}-{}, i ={}, remain = {}, cost = {}ms",
                            range.getIndex(), range.getRanges(), beforeOffset, currOffset, i, range.getEnd() - currOffset, System.currentTimeMillis() - ts);
                    if (beforeOffset == currOffset) {
                        break;
                    }
                }
                super.complete(null);
            } catch (Throwable e) {
                super.completeExceptionally(e);
                Long minOffset = minOffset(runnableList);
                if (service.sendMessage) {
                    service.sendError(service.startupServer.getMessageService(), e, this, minOffset, onlyFieldNameSet, adapter, config, onlyCurrentIndex, sqlWhere, insertIgnore);
                }
                throw e;
            } finally {
                log.info("all sync done threadIndex {}/{}, offset = {}, i ={}, maxId = {}, info ={} ",
                        range.getIndex(), range.getRanges(), currOffset, i, range.getEnd(), this);
                done();
            }
        }

        public void done() {

        }

        public abstract long run0(long offset);

        public Util.Range getRange() {
            return range;
        }

        @Override
        public String toString() {
            return range.getIndex() + ":" + range.getRanges() + "{" +
                    "start=" + range.getStart() +
                    ", end=" + currOffset +
                    ", max=" + range.getEnd() +
                    '}';
        }
    }
}
