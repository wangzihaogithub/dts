package com.github.dts.impl.elasticsearch;

import com.github.dts.cluster.DiscoveryService;
import com.github.dts.cluster.MessageTypeEnum;
import com.github.dts.cluster.SdkInstanceClient;
import com.github.dts.cluster.SdkMessage;
import com.github.dts.impl.elasticsearch.basic.ESSyncConfigSQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * ES外部适配器
 */
public class ESAdapter implements Adapter {
    private static final Map<Object, ReferenceCounted<CacheMap>> IDENTITY_CACHE_MAP = Collections.synchronizedMap(new IdentityHashMap<>());
    private static final Logger log = LoggerFactory.getLogger(ESAdapter.class);
    private final Map<String, ESSyncConfig> esSyncConfig = new ConcurrentHashMap<>(); // 文件名对应配置
    private final Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置
    private final List<ESSyncServiceListener> listenerList = new ArrayList<>();
    private ESConnection esConnection;
    private CanalConfig.OuterAdapterConfig configuration;
    private DefaultESTemplate esTemplate;
    private ExecutorService listenerExecutor;
    private Executor slaveTableExecutor;
    private Executor mainJoinTableExecutor;
    private BasicFieldWriter basicFieldWriter;
    private NestedFieldWriter nestedFieldWriter;
    @Value("${spring.profiles.active:}")
    private String env;
    private boolean refresh = true;
    private boolean onlyEffect = true;
    private int refreshThreshold = 10;
    private int joinUpdateSize = 10;
    private int streamChunkSize = 10000;
    private int maxIdIn = 1000;
    private RealtimeListener connectorCommitListener;

    @Override
    public void init(CanalConfig.CanalAdapter canalAdapter,
                     CanalConfig.OuterAdapterConfig configuration, Properties envProperties,
                     DiscoveryService discoveryService) {
        this.configuration = configuration;
        this.connectorCommitListener = discoveryService == null ? null : new RealtimeListener(discoveryService, configuration.getName(), configuration.getEs().getCommitEventPublishScheduledTickMs(), configuration.getEs().getCommitEventPublishMaxBlockCount());
        this.onlyEffect = configuration.getEs().isOnlyEffect();
        this.joinUpdateSize = configuration.getEs().getJoinUpdateSize();
        this.streamChunkSize = configuration.getEs().getStreamChunkSize();
        this.refresh = configuration.getEs().isRefresh();
        this.refreshThreshold = configuration.getEs().getRefreshThreshold();
        this.esConnection = new ESConnection(configuration.getEs());
        this.esTemplate = new DefaultESTemplate(esConnection);
        this.maxIdIn = configuration.getEs().getMaxIdIn();
        this.basicFieldWriter = new BasicFieldWriter(esTemplate);
        String clientIdentity = canalAdapter.clientIdentity();

        this.listenerExecutor = Util.newFixedThreadPool(
                configuration.getEs().getListenerThreads(),
                60_000L, "ESListener-" + clientIdentity, false);
        CanalConfig.OuterAdapterConfig.Es.SlaveNestedField slaveNestedField = configuration.getEs().getSlaveNestedField();
        this.slaveTableExecutor = slaveNestedField.isBlock() ?
                Runnable::run :
                Util.newFixedThreadPool(1, slaveNestedField.getThreads(),
                        60_000L, "ESNestJoinS-" + clientIdentity, true, false, slaveNestedField.getQueues(), NestedSlaveTableRunnable::merge);
        CanalConfig.OuterAdapterConfig.Es.MainJoinNestedField mainJoinNestedField = configuration.getEs().getMainJoinNestedField();

        this.mainJoinTableExecutor = mainJoinNestedField.isBlock() ?
                Runnable::run :
                Util.newFixedThreadPool(1, mainJoinNestedField.getThreads(),
                        60_000L, "ESJoin-" + clientIdentity, true, false, mainJoinNestedField.getQueues(), NestedMainJoinTableRunnable::merge);
        ESSyncConfig.loadESSyncConfig(dbTableEsSyncConfig, esSyncConfig, envProperties, canalAdapter, configuration.getName(), configuration.getEs().resourcesDir(), env);

        this.listenerList.sort(AnnotationAwareOrderComparator.INSTANCE);
        this.listenerList.forEach(item -> item.init(esSyncConfig));
        this.nestedFieldWriter = new NestedFieldWriter(configuration.getEs().getNestedFieldThreads(),
                "ESNestJoinM-", esSyncConfig, esTemplate);
    }

    private ReferenceCounted<CacheMap> newCacheMap(Object id) {
        Function<Object, ReferenceCounted<CacheMap>> cacheFactory = k -> new ReferenceCounted<CacheMap>(new CacheMap(configuration.getEs().getMaxQueryCacheSize())) {
            @Override
            public void close() {
                super.close();
                if (refCnt() == 0) {
                    IDENTITY_CACHE_MAP.remove(k);
                    get().cacheClear();
                }
            }
        };
        if (configuration.getEs().isShareAdapterCache()) {
            while (true) {
                ReferenceCounted<CacheMap> counted = IDENTITY_CACHE_MAP.computeIfAbsent(id, cacheFactory);
                try {
                    return counted.open();
                } catch (IllegalStateException e) {
                    log.warn("newCacheMap counted.open() fail {}", e.toString());
                    Thread.yield();
                }
            }
        } else {
            return cacheFactory.apply(id);
        }
    }

    public Map<String, ESSyncConfig> getEsSyncConfigByIndex(String index) {
        Map<String, ESSyncConfig> map = new LinkedHashMap<>();
        for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
            if (entry.getValue().getEsMapping().get_index().equals(index)) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    public Map<String, ESSyncConfig> getEsSyncConfigByDestination(String destination) {
        Map<String, ESSyncConfig> map = new LinkedHashMap<>();
        for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
            if (entry.getValue().getDestination().equals(destination)) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig() {
        return Collections.unmodifiableMap(esSyncConfig);
    }

    @Override
    public CompletableFuture<Void> sync(List<Dml> dmls) {
        return sync(dmls, refresh, onlyEffect, false, joinUpdateSize, null, connectorCommitListener);
    }

    public <L extends EsDependentCommitListener & ESTemplate.RefreshListener> CompletableFuture<Void> sync(List<Dml> dmls, boolean refresh, boolean onlyEffect,
                                                                                                           boolean onlyCurrentIndex, int joinUpdateSize,
                                                                                                           Collection<String> onlyFieldName,
                                                                                                           L commitListener) {
        if (dmls == null || dmls.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // share Adapter Cache
        ReferenceCounted<CacheMap> cacheMapRef = newCacheMap(dmls);
        CacheMap cacheMap = cacheMapRef.get();
        try {
            ESTemplate.BulkRequestList bulkRequestList = esTemplate.newBulkRequestList(BulkPriorityEnum.HIGH);
            List<Dml> syncDmlList = dmls.stream().filter(e -> !Boolean.TRUE.equals(e.getIsDdl())).collect(Collectors.toList());
            String tables = syncDmlList.stream().map(Dml::getTable).distinct().collect(Collectors.joining(","));
            Dml max = syncDmlList.stream().filter(e -> e.getEs() != null).max(Comparator.comparing(Dml::getEs)).orElse(null);
            Timestamp maxSqlEsTimestamp = max == null ? null : new Timestamp(max.getEs());

            // basic field change
            BasicFieldWriter.WriteResult writeResult = new BasicFieldWriter.WriteResult();
            Timestamp startTimestamp = new Timestamp(System.currentTimeMillis());
            long timestamp = System.currentTimeMillis();
            long basicFieldWriterCost;
            try {
                Map<Map<String, ESSyncConfig>, List<Dml>> groupByMap = new IdentityHashMap<>(dbTableEsSyncConfig.size());
                for (Dml dml : syncDmlList) {
                    for (String destination : dml.getDestination()) {
                        String key = ESSyncConfig.getEsSyncConfigKey(destination, dml.getDatabase(), dml.getTable());
                        Map<String, ESSyncConfig> configMap = dbTableEsSyncConfig.get(key);
                        if (configMap != null) {
                            groupByMap.computeIfAbsent(configMap, e -> new ArrayList<>()).add(dml);
                        }
                    }
                }
                for (Map.Entry<Map<String, ESSyncConfig>, List<Dml>> entry : groupByMap.entrySet()) {
                    BasicFieldWriter.WriteResult item = basicFieldWriter.writeEsReturnSql(entry.getKey().values(), entry.getValue(), bulkRequestList);
                    writeResult.add(item);
                }
                BasicFieldWriter.executeUpdate(writeResult.getSqlList(), maxIdIn);
            } finally {
                basicFieldWriterCost = System.currentTimeMillis() - timestamp;
                bulkRequestList.commit(esTemplate, null);
                if (refresh) {
                    refresh(writeResult.getIndices(), commitListener);
                }
            }
            writeResult.trimToSize();

            timestamp = System.currentTimeMillis();
            SqlDependentGroup sqlDependentGroup = nestedFieldWriter.convertToSqlDependentGroup(syncDmlList, onlyCurrentIndex, onlyEffect);

            // join async change
            List<CompletableFuture<?>> futures = asyncRunSqlDependent(maxSqlEsTimestamp, joinUpdateSize,
                    bulkRequestList, sqlDependentGroup.selectMainTableJoinDependentList(), sqlDependentGroup.selectSlaveTableDependentList(), cacheMap);

            // nested type ： main table change
            List<SqlDependent> mainTableSqlDependentList = sqlDependentGroup.selectMainTableSqlDependentList(onlyFieldName);
            boolean changeNestedFieldMainTable = !mainTableSqlDependentList.isEmpty();
            if (changeNestedFieldMainTable) {
                try {
                    nestedFieldWriter.writeMainTable(mainTableSqlDependentList, bulkRequestList, maxIdIn, cacheMap);
                } finally {
                    log.info("sync(dml[{}]).BasicFieldWriter={}ms, NestedFieldWriter={}ms, {}, table={}",
                            syncDmlList.size(),
                            basicFieldWriterCost,
                            System.currentTimeMillis() - timestamp,
                            maxSqlEsTimestamp, tables);
                    bulkRequestList.commit(esTemplate, null);
                }
            }

            // call listeners
            boolean changeListenerList = !listenerList.isEmpty();
            if (changeListenerList) {
                try {
                    invokeAllListener(syncDmlList, bulkRequestList);
                } catch (InterruptedException | ExecutionException e) {
                    Util.sneakyThrows(e);
                } finally {
                    bulkRequestList.commit(esTemplate, null); // 批次统一提交
                }
            }

            // future
            CompletableFuture<Void> future = allOfCompleted(futures, commitListener, sqlDependentGroup, writeResult, startTimestamp, maxSqlEsTimestamp, refresh);
            // refresh
            if (refresh && dmls.size() < refreshThreshold) {
                if (changeListenerList || changeNestedFieldMainTable) {
                    refresh(sqlDependentGroup.getIndices(), commitListener);
                }
            }
            // close cache
            future.thenAccept(unused -> cacheMapRef.close());
            return future;
        } catch (Exception e) {
            cacheMapRef.close();
            throw e;
        }
    }

    private <L extends EsDependentCommitListener & ESTemplate.RefreshListener> CompletableFuture<Void> allOfCompleted(
            List<CompletableFuture<?>> futures, L commitListener, SqlDependentGroup sqlDependentGroup,
            BasicFieldWriter.WriteResult writeResult,
            Timestamp startTimestamp, Timestamp sqlEsTimestamp,
            boolean refresh) {
        if (futures.isEmpty()) {
            if (commitListener != null) {
                commitListener.done(new EsDependentCommitListener.CommitEvent(writeResult, sqlDependentGroup.getMainTableDependentList(),
                        sqlDependentGroup.getMainTableJoinDependentList(),
                        sqlDependentGroup.getSlaveTableDependentList(),
                        startTimestamp,
                        sqlEsTimestamp
                ));
            }
            return CompletableFuture.completedFuture(null);
        } else {
            if (commitListener != null) {
                commitListener.done(new EsDependentCommitListener.CommitEvent(writeResult, sqlDependentGroup.getMainTableDependentList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        startTimestamp,
                        sqlEsTimestamp
                ));
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            future.completeExceptionally(throwable);
                        } else {
                            future.complete(null);
                            if (commitListener != null) {
                                commitListener.done(new EsDependentCommitListener.CommitEvent(null, Collections.emptyList(),
                                        sqlDependentGroup.getMainTableJoinDependentList(),
                                        sqlDependentGroup.getSlaveTableDependentList(),
                                        startTimestamp,
                                        sqlEsTimestamp
                                ));
                            }
                            if (refresh) {
                                refresh(sqlDependentGroup.getIndices(), commitListener);
                            }
                        }
                    });
            return future;
        }
    }

    private void refresh(Collection<String> indices, ESTemplate.RefreshListener listener) {
        if (indices.isEmpty()) {
            return;
        }
        esTemplate.refresh(indices).whenComplete((response, throwable) -> {
            if (throwable != null) {
                listener.done(response);
            }
        });
    }

    private List<CompletableFuture<?>> asyncRunSqlDependent(Timestamp maxTimestamp,
                                                            int joinUpdateSize,
                                                            ESTemplate.BulkRequestList bulkRequestList,
                                                            List<SqlDependent> mainTableJoinSqlDependentList,
                                                            List<SqlDependent> slaveTableSqlDependentList,
                                                            CacheMap cacheMap) {
        List<CompletableFuture<?>> futures = new ArrayList<>();

        // nested type ：main table parent object change
        if (!mainTableJoinSqlDependentList.isEmpty()) {
            NestedMainJoinTableRunnable runnable = new NestedMainJoinTableRunnable(
                    mainTableJoinSqlDependentList, esTemplate,
                    bulkRequestList,
                    null,
                    joinUpdateSize, streamChunkSize, maxTimestamp);
            futures.add(runnable);
            mainJoinTableExecutor.execute(runnable);
        }

        // nested type ：join table change
        List<SqlDependent> slaveTableList = slaveTableSqlDependentList.stream()
                .filter(e -> e.getDml().isTypeUpdate())
                .collect(Collectors.toList());
        if (!slaveTableList.isEmpty()) {
            NestedSlaveTableRunnable runnable = new NestedSlaveTableRunnable(
                    slaveTableList, esTemplate,
                    bulkRequestList,
                    null,
                    cacheMap, maxTimestamp, maxIdIn, streamChunkSize);
            futures.add(runnable);
            slaveTableExecutor.execute(runnable);
        }
        return futures;
    }

    private void invokeAllListener(List<Dml> dmls, ESTemplate.BulkRequestList bulkRequestList) throws InterruptedException, ExecutionException {
        List<Future> futures = new ArrayList<>();
        for (ESSyncServiceListener listener : listenerList) {
            futures.add(listenerExecutor.submit(() -> {
                try {
                    listener.onSyncAfter(dmls, this, bulkRequestList);
                } catch (Exception e) {
                    log.error("onSyncAfter error. case = {} listener = {}, dml = {}", e.getMessage(), listener, dmls, e);
                    throw e;
                }
            }));
        }
        for (Future<Object> future : futures) {
            future.get();
        }
    }

    @Override
    public void destroy() {
        if (listenerExecutor != null) {
            listenerExecutor.shutdown();
        }
        if (esConnection != null) {
            esConnection.close();
        }
    }

    @Override
    public CanalConfig.OuterAdapterConfig getConfiguration() {
        return configuration;
    }

    @Autowired(required = false)
    public void setListenerList(Collection<ESSyncServiceListener> listenerList) {
        this.listenerList.addAll(listenerList);
    }

    public DefaultESTemplate getEsTemplate() {
        return esTemplate;
    }

    public boolean isOnlyEffect() {
        return onlyEffect;
    }

    public void setOnlyEffect(boolean onlyEffect) {
        this.onlyEffect = onlyEffect;
    }

    public boolean isRefresh() {
        return refresh;
    }

    public int getRefreshThreshold() {
        return refreshThreshold;
    }

    public void setRefreshThreshold(int refreshThreshold) {
        this.refreshThreshold = refreshThreshold;
    }

    public List<Map<String, ESSyncConfig>> getEsSyncConfig(String database, String table) {
        String[] destination = getDestination();
        List<Map<String, ESSyncConfig>> list = new ArrayList<>();
        for (String s : destination) {
            Map<String, ESSyncConfig> configMap = dbTableEsSyncConfig.get(ESSyncConfig.getEsSyncConfigKey(s, database, table));
            if (configMap != null) {
                list.add(configMap);
            }
        }
        return list;
    }

    @Override
    public String toString() {
        return configuration.getName() + "{" +
                "address='" + Arrays.toString(configuration.getEs().getAddress()) + '\'' +
                ", refresh=" + refresh +
                ", onlyEffect=" + onlyEffect +
                ", refreshThreshold=" + refreshThreshold +
                ", joinUpdateSize=" + joinUpdateSize +
                ", streamChunkSize=" + streamChunkSize +
                ", maxIdIn=" + maxIdIn +
                '}';
    }

    static class RealtimeListener implements ESTemplate.RefreshListener, EsDependentCommitListener, Runnable {
        private static final Function<Dml, Map<Integer, ArrayList<SqlDependent>>> MAPPING_FUNCTION = k -> new LinkedHashMap<>(2);
        private static final Function<Integer, ArrayList<SqlDependent>> MAPPING_FUNCTION1 = k -> new ArrayList<>(2);
        private static volatile ScheduledExecutorService SCHEDULED;
        private final DiscoveryService discoveryService;
        private final LinkedBlockingQueue<CommitEvent> eventList;
        private final String adapterName;

        RealtimeListener(DiscoveryService discoveryService, String adapterName, int commitEventPublishScheduledTickMs, int commitEventPublishMaxBlockCount) {
            this.discoveryService = discoveryService;
            this.adapterName = adapterName;
            this.eventList = new LinkedBlockingQueue<>(commitEventPublishMaxBlockCount);
            getScheduled().scheduleWithFixedDelay(this, commitEventPublishScheduledTickMs, commitEventPublishScheduledTickMs, TimeUnit.MILLISECONDS);
        }

        public static ScheduledExecutorService getScheduled() {
            if (SCHEDULED == null) {
                synchronized (RealtimeListener.class) {
                    if (SCHEDULED == null) {
                        SCHEDULED = Util.newScheduled(1, () -> "RealtimeListener", e -> log.warn("Scheduled error {}", e.toString(), e));
                    }
                }
            }
            return SCHEDULED;
        }

        private static void putDependent(Map<Dml, Map<Integer, ArrayList<SqlDependent>>> dependentMap, SqlDependent sqlDependent) {
            dependentMap.computeIfAbsent(sqlDependent.getDml(), MAPPING_FUNCTION)
                    .computeIfAbsent(sqlDependent.getDml().getIndex(), MAPPING_FUNCTION1)
                    .add(sqlDependent);
        }

        private static IdentityHashMap<Dml, Map<Integer, ArrayList<SqlDependent>>> trimTableSize(IdentityHashMap<Dml, Map<Integer, ArrayList<SqlDependent>>> dependentMap) {
            for (Map<Integer, ArrayList<SqlDependent>> value : dependentMap.values()) {
                for (ArrayList<SqlDependent> sqlDependents : value.values()) {
                    sqlDependents.trimToSize();
                }
            }
            return new IdentityHashMap<>(dependentMap);
        }

        private static Map<Dml, Map<Integer, ArrayList<SqlDependent>>> groupByDml(List<CommitEvent> list) {
            IdentityHashMap<Dml, Map<Integer, ArrayList<SqlDependent>>> dependentMap = new IdentityHashMap<>();
            for (CommitEvent event : list) {
                for (SqlDependent sqlDependent : event.mainTableSqlDependentList) {
                    putDependent(dependentMap, sqlDependent);
                }
                for (SqlDependent sqlDependent : event.mainTableJoinSqlDependentList) {
                    putDependent(dependentMap, sqlDependent);
                }
                for (SqlDependent sqlDependent : event.slaveTableSqlDependentList) {
                    putDependent(dependentMap, sqlDependent);
                }
                BasicFieldWriter.WriteResult writeResult = event.writeResult;
                if (writeResult != null) {
                    for (ESSyncConfigSQL configSQL : writeResult.getSqlList()) {
                        putDependent(dependentMap, configSQL.getDependent());
                    }
                    for (SqlDependent sqlDependent : writeResult.getInsertList()) {
                        putDependent(dependentMap, sqlDependent);
                    }
                    for (SqlDependent sqlDependent : writeResult.getUpdateList()) {
                        putDependent(dependentMap, sqlDependent);
                    }
                    for (SqlDependent sqlDependent : writeResult.getDeleteList()) {
                        putDependent(dependentMap, sqlDependent);
                    }
                }
            }
            // 减少List默认16Size的内存占用
            return trimTableSize(dependentMap);
        }

        private static List<DmlDTO> convert(Map<Dml, Map<Integer, ArrayList<SqlDependent>>> dmlListMap, String adapterName) {
            List<DmlDTO> list = new ArrayList<>();
            for (Map.Entry<Dml, Map<Integer, ArrayList<SqlDependent>>> entry : dmlListMap.entrySet()) {
                Dml dml = entry.getKey();
                for (Map.Entry<Integer, ArrayList<SqlDependent>> entry1 : entry.getValue().entrySet()) {
                    List<SqlDependent> sqlDependentList = entry1.getValue();
                    SqlDependent f = sqlDependentList.get(0);
                    List<DmlDTO.Dependent> descList = convertDtoDependentList(sqlDependentList);
                    DmlDTO dmlDTO = new DmlDTO();
                    dmlDTO.setTableName(dml.getTable());
                    dmlDTO.setDatabase(dml.getDatabase());
                    dmlDTO.setPkNames(dml.getPkNames());
                    dmlDTO.setEs(dml.getEs());
                    dmlDTO.setTs(dml.getTs());
                    dmlDTO.setType(dml.getType());
                    dmlDTO.setOld(f.getOldMap());
                    dmlDTO.setData(f.getDataMap());
                    dmlDTO.setDependents(descList);
                    dmlDTO.setAdapterName(adapterName);
                    list.add(dmlDTO);
                }
            }
            return list;
        }

        private static List<DmlDTO.Dependent> convertDtoDependentList(List<SqlDependent> sqlDependentList) {
            List<DmlDTO.Dependent> descList = new ArrayList<>(sqlDependentList.size());
            for (SqlDependent sqlDependent : sqlDependentList) {
                SchemaItem schemaItem = sqlDependent.getSchemaItem();
                DmlDTO.Dependent desc = new DmlDTO.Dependent();
                desc.setEffect(sqlDependent.isEffect());
                desc.setName(schemaItem.getDesc());
                desc.setEsIndex(schemaItem.getEsMapping().get_index());
                descList.add(desc);
            }
            return descList;
        }

        @Override
        public void done(CommitEvent event) {
            try {
                eventList.put(event);
            } catch (InterruptedException e) {
                Util.sneakyThrows(e);
            }
        }

        @Override
        public void done(ESBulkRequest.EsRefreshResponse response) {
            run();
        }

        private List<CommitEvent> drainTo() {
            ArrayList<CommitEvent> list = new ArrayList<>(eventList.size());
            eventList.drainTo(list);
            return list;
        }

        @Override
        public void run() {
            if (eventList.isEmpty()) {
                return;
            }
            List<SdkInstanceClient> configSdkUnmodifiableList = discoveryService.getConfigSdkUnmodifiableList();
            try (ReferenceCounted<List<SdkInstanceClient>> sdkListRef = discoveryService.getSdkListRef()) {
                List<SdkInstanceClient> clientList = sdkListRef.get();
                if (clientList.isEmpty() && configSdkUnmodifiableList.isEmpty()) {
                    return;
                }

                DiscoveryService.MessageIdIncrementer messageIdIncrementer = discoveryService.getMessageIdIncrementer();
                Map<Dml, Map<Integer, ArrayList<SqlDependent>>> dmlListMap = groupByDml(drainTo());
                List<DmlDTO> dmlDTO = convert(dmlListMap, adapterName);
                try {
                    for (DmlDTO dto : dmlDTO) {
                        long id = messageIdIncrementer.incrementId(dmlDTO.size());
                        SdkMessage sdkMessage = new SdkMessage(id, MessageTypeEnum.ES_DML, dto);
                        for (SdkInstanceClient client : clientList) {
                            client.write(sdkMessage);
                        }
                        for (SdkInstanceClient client : configSdkUnmodifiableList) {
                            client.write(sdkMessage);
                        }
                    }
                } finally {
                    for (SdkInstanceClient client : clientList) {
                        client.flush();
                    }
                    for (SdkInstanceClient client : configSdkUnmodifiableList) {
                        client.flush();
                    }
                }
            }
        }
    }
}
