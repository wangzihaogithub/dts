package com.github.dts.impl.elasticsearch7x;

import com.github.dts.cluster.DiscoveryService;
import com.github.dts.cluster.MessageTypeEnum;
import com.github.dts.cluster.SdkInstanceClient;
import com.github.dts.cluster.SdkMessage;
import com.github.dts.impl.elasticsearch7x.basic.ESSyncConfigSQL;
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
public class ES7xAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(ES7xAdapter.class);
    private final Map<String, ESSyncConfig> esSyncConfig = new ConcurrentHashMap<>(); // 文件名对应配置
    private final Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置
    private final List<ESSyncServiceListener> listenerList = new ArrayList<>();
    private CacheMap cacheMap;
    private ES7xConnection esConnection;
    private CanalConfig.OuterAdapterConfig configuration;
    private ES7xTemplate esTemplate;
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
        this.connectorCommitListener = discoveryService == null ? null : new RealtimeListener(discoveryService, configuration.getEs7x().getCommitEventPublishScheduledTickMs(), configuration.getEs7x().getCommitEventPublishMaxBlockCount());
        this.onlyEffect = configuration.getEs7x().isOnlyEffect();
        this.cacheMap = new CacheMap(configuration.getEs7x().getMaxQueryCacheSize());
        this.joinUpdateSize = configuration.getEs7x().getJoinUpdateSize();
        this.streamChunkSize = configuration.getEs7x().getStreamChunkSize();
        this.refresh = configuration.getEs7x().isRefresh();
        this.refreshThreshold = configuration.getEs7x().getRefreshThreshold();
        this.esConnection = new ES7xConnection(configuration.getEs7x());
        this.esTemplate = new ES7xTemplate(esConnection);
        this.maxIdIn = configuration.getEs7x().getMaxIdIn();
        this.basicFieldWriter = new BasicFieldWriter(esTemplate);
        this.listenerExecutor = Util.newFixedThreadPool(
                configuration.getEs7x().getListenerThreads(),
                60_000L, "ES-listener", false);
        CanalConfig.OuterAdapterConfig.Es7x.SlaveNestedField slaveNestedField = configuration.getEs7x().getSlaveNestedField();
        this.slaveTableExecutor = slaveNestedField.isBlock() ?
                Runnable::run :
                Util.newFixedThreadPool(1, slaveNestedField.getThreads(),
                        60_000L, "ESNestedSlave", true, false, slaveNestedField.getQueues(), NestedSlaveTableRunnable::merge);
        CanalConfig.OuterAdapterConfig.Es7x.MainJoinNestedField mainJoinNestedField = configuration.getEs7x().getMainJoinTableField();
        this.mainJoinTableExecutor = mainJoinNestedField.isBlock() ?
                Runnable::run :
                Util.newFixedThreadPool(1, mainJoinNestedField.getThreads(),
                        60_000L, "ESNestedMainJoin", true, false, mainJoinNestedField.getQueues(), NestedMainJoinTableRunnable::merge);
        ESSyncUtil.loadESSyncConfig(dbTableEsSyncConfig, esSyncConfig, envProperties, configuration.getEs7x().resourcesDir(), env);

        this.listenerList.sort(AnnotationAwareOrderComparator.INSTANCE);
        this.listenerList.forEach(item -> item.init(esSyncConfig));
        this.nestedFieldWriter = new NestedFieldWriter(configuration.getEs7x().getNestedFieldThreads(), esSyncConfig, esTemplate, cacheMap);
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
                for (String s : dml.getDestination()) {
                    String key = ESSyncUtil.getEsSyncConfigKey(s, dml.getDatabase(), dml.getTable());
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
            cacheMap.cacheClear();
            bulkRequestList.commit(esTemplate, null);
            if (refresh) {
                refresh(writeResult.getIndices(), commitListener);
            }
        }

        timestamp = System.currentTimeMillis();
        DependentGroup dependentGroup = nestedFieldWriter.convertToDependentGroup(syncDmlList, onlyCurrentIndex, onlyEffect);

        // join async change
        List<CompletableFuture<?>> futures = asyncRunDependent(maxSqlEsTimestamp, joinUpdateSize,
                bulkRequestList, dependentGroup.getMainTableJoinDependentList(), dependentGroup.getSlaveTableDependentList());

        // nested type ： main table change
        List<Dependent> mainTableDependentList = dependentGroup.selectMainTableDependentList(onlyFieldName);
        boolean changeNestedFieldMainTable = !mainTableDependentList.isEmpty();
        if (changeNestedFieldMainTable) {
            try {
                nestedFieldWriter.writeMainTable(mainTableDependentList, bulkRequestList, maxIdIn);
            } finally {
                log.info("sync(dml[{}]).BasicFieldWriter={}ms, NestedFieldWriter={}ms, {}, table={}",
                        syncDmlList.size(),
                        basicFieldWriterCost,
                        System.currentTimeMillis() - timestamp,
                        maxSqlEsTimestamp, tables);
                cacheMap.cacheClear();
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
                cacheMap.cacheClear();
                bulkRequestList.commit(esTemplate, null); // 批次统一提交
            }
        }

        // future
        CompletableFuture<Void> future = allOfCompleted(futures, commitListener, dependentGroup, writeResult, startTimestamp, maxSqlEsTimestamp, refresh);
        // refresh
        if (refresh && dmls.size() < refreshThreshold) {
            if (changeListenerList || changeNestedFieldMainTable) {
                refresh(dependentGroup.getIndices(), commitListener);
            }
        }
        return future;
    }

    private <L extends EsDependentCommitListener & ESTemplate.RefreshListener> CompletableFuture<Void> allOfCompleted(
            List<CompletableFuture<?>> futures, L commitListener, DependentGroup dependentGroup,
            BasicFieldWriter.WriteResult writeResult,
            Timestamp startTimestamp, Timestamp sqlEsTimestamp,
            boolean refresh) {
        if (futures.isEmpty()) {
            if (commitListener != null) {
                commitListener.done(new EsDependentCommitListener.CommitEvent(writeResult, dependentGroup.getMainTableDependentList(),
                        dependentGroup.getMainTableJoinDependentList(),
                        dependentGroup.getSlaveTableDependentList(),
                        startTimestamp,
                        sqlEsTimestamp
                ));
            }
            return CompletableFuture.completedFuture(null);
        } else {
            if (commitListener != null) {
                commitListener.done(new EsDependentCommitListener.CommitEvent(writeResult, dependentGroup.getMainTableDependentList(),
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
                                        dependentGroup.getMainTableJoinDependentList(),
                                        dependentGroup.getSlaveTableDependentList(),
                                        startTimestamp,
                                        sqlEsTimestamp
                                ));
                            }
                            if (refresh) {
                                refresh(dependentGroup.getIndices(), commitListener);
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

    private List<CompletableFuture<?>> asyncRunDependent(Timestamp maxTimestamp,
                                                         int joinUpdateSize,
                                                         ESTemplate.BulkRequestList bulkRequestList,
                                                         List<Dependent> mainTableJoinDependentList,
                                                         List<Dependent> slaveTableDependentList) {
        List<CompletableFuture<?>> futures = new ArrayList<>();

        // nested type ：main table parent object change
        if (!mainTableJoinDependentList.isEmpty()) {
            NestedMainJoinTableRunnable runnable = new NestedMainJoinTableRunnable(
                    mainTableJoinDependentList, esTemplate,
                    bulkRequestList,
                    null,
                    joinUpdateSize, streamChunkSize, maxTimestamp);
            futures.add(runnable);
            mainJoinTableExecutor.execute(runnable);
        }

        // nested type ：join table change
        List<Dependent> slaveTableList = slaveTableDependentList.stream()
                .filter(e -> e.getDml().isTypeUpdate())
                .collect(Collectors.toList());
        if (!slaveTableList.isEmpty()) {
            NestedSlaveTableRunnable runnable = new NestedSlaveTableRunnable(
                    slaveTableList, esTemplate,
                    bulkRequestList,
                    null,
                    cacheMap.getMaxValueSize(), maxTimestamp, maxIdIn, streamChunkSize);
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

    public ES7xTemplate getEsTemplate() {
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
            Map<String, ESSyncConfig> configMap = dbTableEsSyncConfig.get(ESSyncUtil.getEsSyncConfigKey(s, database, table));
            if (configMap != null) {
                list.add(configMap);
            }
        }
        return list;
    }

    static class RealtimeListener implements ESTemplate.RefreshListener, EsDependentCommitListener, Runnable {
        private static volatile ScheduledExecutorService SCHEDULED;
        private final DiscoveryService discoveryService;
        private final LinkedBlockingQueue<CommitEvent> eventList;

        RealtimeListener(DiscoveryService discoveryService, int commitEventPublishScheduledTickMs, int commitEventPublishMaxBlockCount) {
            this.discoveryService = discoveryService;
            this.eventList = new LinkedBlockingQueue<>(commitEventPublishMaxBlockCount);
            getScheduled().scheduleWithFixedDelay(this, commitEventPublishScheduledTickMs, commitEventPublishMaxBlockCount, TimeUnit.MILLISECONDS);
        }

        public static ScheduledExecutorService getScheduled() {
            if (SCHEDULED == null) {
                synchronized (RealtimeListener.class) {
                    if (SCHEDULED == null) {
                        SCHEDULED = Util.newScheduled(1, "RealtimeListener", true);
                    }
                }
            }
            return SCHEDULED;
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
            eventList.drainTo(list, 10000);
            return list;
        }

        @Override
        public void run() {
            if (eventList.isEmpty()) {
                return;
            }
            try (ReferenceCounted<List<SdkInstanceClient>> sdkListRef = discoveryService.getSdkListRef()) {
                List<SdkInstanceClient> clientList = sdkListRef.get();
                if (clientList.isEmpty()) {
                    return;
                }

                DiscoveryService.MessageIdIncrementer messageIdIncrementer = discoveryService.getMessageIdIncrementer();
                Map<Dml, Map<Integer, List<Dependent>>> dmlListMap = groupByDml(drainTo());
                List<DmlDTO> dmlDTO = convert(dmlListMap);
                try {
                    for (DmlDTO dto : dmlDTO) {
                        long id = messageIdIncrementer.incrementId(dmlDTO.size());
                        SdkMessage sdkMessage = new SdkMessage(id, MessageTypeEnum.ES_DML, dto);
                        for (SdkInstanceClient client : clientList) {
                            client.write(sdkMessage);
                        }
                    }
                } finally {
                    for (SdkInstanceClient client : clientList) {
                        client.flush();
                    }
                }
            }
        }

        private String dependentKey(Dependent dependent) {
            return dependent.getDml() + "_" + dependent.getIndex();
        }

        private Map<Dml, Map<Integer, List<Dependent>>> groupByDml(List<CommitEvent> list) {
            Map<String, List<Dependent>> dependentMap = new IdentityHashMap<>();
            Function<String, List<Dependent>> mappingFunction = k -> new ArrayList<>();
            for (CommitEvent event : list) {
                for (Dependent dependent : event.mainTableDependentList) {
                    dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                            .add(dependent);
                }
                for (Dependent dependent : event.mainTableJoinDependentList) {
                    dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                            .add(dependent);
                }
                for (Dependent dependent : event.slaveTableDependentList) {
                    dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                            .add(dependent);
                }
                BasicFieldWriter.WriteResult writeResult = event.writeResult;
                if (writeResult != null) {
                    for (ESSyncConfigSQL configSQL : writeResult.getSqlList()) {
                        Dependent dependent = configSQL.getDependent();
                        dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                                .add(dependent);
                    }
                    for (Dependent dependent : writeResult.getInsertList()) {
                        dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                                .add(dependent);
                    }
                    for (Dependent dependent : writeResult.getUpdateList()) {
                        dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                                .add(dependent);
                    }
                    for (Dependent dependent : writeResult.getDeleteList()) {
                        dependentMap.computeIfAbsent(dependentKey(dependent), mappingFunction)
                                .add(dependent);
                    }
                }
            }
            Map<Dml, Map<Integer, List<Dependent>>> dmlListMap = new IdentityHashMap<>();
            for (List<Dependent> value : dependentMap.values()) {
                Dependent dependent = value.get(0);
                dmlListMap.computeIfAbsent(dependent.getDml(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(dependent.getIndex(), k -> new ArrayList<>())
                        .addAll(value);
            }
            return dmlListMap;
        }

        private List<DmlDTO> convert(Map<Dml, Map<Integer, List<Dependent>>> dmlListMap) {
            List<DmlDTO> list = new ArrayList<>();
            for (Map.Entry<Dml, Map<Integer, List<Dependent>>> entry : dmlListMap.entrySet()) {
                Dml dml = entry.getKey();
                for (Map.Entry<Integer, List<Dependent>> entry1 : entry.getValue().entrySet()) {
                    List<Dependent> value = entry1.getValue();
                    Dependent f = value.get(0);
                    Set<String> desc = new LinkedHashSet<>();
                    Set<String> indexs = new LinkedHashSet<>();
                    for (Dependent dependent : value) {
                        SchemaItem schemaItem = dependent.getSchemaItem();
                        indexs.add(schemaItem.getEsMapping().get_index());
                        desc.add(schemaItem.getDesc());
                    }
                    DmlDTO dmlDTO = new DmlDTO();
                    dmlDTO.setTableName(dml.getTable());
                    dmlDTO.setDatabase(dml.getDatabase());
                    dmlDTO.setPkNames(dml.getPkNames());
                    dmlDTO.setEs(dml.getEs());
                    dmlDTO.setTs(dml.getTs());
                    dmlDTO.setType(dml.getType());
                    dmlDTO.setOld(f.getOldMap());
                    dmlDTO.setData(f.getDataMap());
                    dmlDTO.setIndexNames(new ArrayList<>(indexs));
                    dmlDTO.setDesc(new ArrayList<>(desc));
                    list.add(dmlDTO);
                }
            }
            return list;
        }
    }

}
