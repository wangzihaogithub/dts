package com.github.dts.impl.elasticsearch7x;

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
import java.util.stream.Collectors;


/**
 * ES外部适配器
 */
public class ES7xAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(ES7xAdapter.class);
    private final Map<String, ESSyncConfig> esSyncConfig = new ConcurrentHashMap<>(); // 文件名对应配置
    private final Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置
    private final List<ESSyncServiceListener> listenerList = new ArrayList<>();
    private final Set<String> allIndices = new LinkedHashSet<>();
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
    private boolean autoUpdateChildren = false;
    private int refreshThreshold = 10;
    private int joinUpdateSize = 10;
    private int streamChunkSize = 10000;
    private int maxIdIn = 1000;

    @Override
    public void init(CanalConfig.CanalAdapter canalAdapter, CanalConfig.OuterAdapterConfig configuration, Properties envProperties) {
        this.configuration = configuration;
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
        for (ESSyncConfig value : esSyncConfig.values()) {
            allIndices.add(value.getEsMapping().get_index());
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
        return sync(dmls, refresh, autoUpdateChildren, false, joinUpdateSize, null);
    }

    public CompletableFuture<Void> sync(List<Dml> dmls, boolean refresh, boolean autoUpdateChildren,
                                        boolean onlyCurrentIndex, int joinUpdateSize, Collection<String> onlyFieldName) {
        if (dmls == null || dmls.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ESTemplate.BulkRequestList bulkRequestList = esTemplate.newBulkRequestList(BulkPriorityEnum.HIGH);
        List<Dml> syncDmlList = dmls.stream().filter(e -> !Boolean.TRUE.equals(e.getIsDdl())).collect(Collectors.toList());
        String tables = syncDmlList.stream().map(Dml::getTable).distinct().collect(Collectors.joining(","));
        Dml max = syncDmlList.stream().filter(e -> e.getEs() != null).max(Comparator.comparing(Dml::getEs)).orElse(null);
        Timestamp maxTimestamp = max == null ? null : new Timestamp(max.getEs());

        Set<String> indices = new LinkedHashSet<>();
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
            List<ESSyncConfigSQL> configSQLList = new ArrayList<>();
            for (Map.Entry<Map<String, ESSyncConfig>, List<Dml>> entry : groupByMap.entrySet()) {
                List<ESSyncConfigSQL> item = basicFieldWriter.writeEsReturnSql(entry.getKey().values(), entry.getValue(), bulkRequestList);
                configSQLList.addAll(item);
                if (!item.isEmpty() && !bulkRequestList.isEmpty()) {
                    for (ESSyncConfig value : entry.getKey().values()) {
                        indices.add(value.getEsMapping().get_index());
                    }
                }
            }
            BasicFieldWriter.executeUpdate(configSQLList, maxIdIn);
        } finally {
            basicFieldWriterCost = System.currentTimeMillis() - timestamp;
            cacheMap.cacheClear();
            bulkRequestList.commit(esTemplate);

            // refresh
            if (refresh) {
                esTemplate.refresh(indices);
            }
        }

        timestamp = System.currentTimeMillis();
        DependentGroup dependentGroup = nestedFieldWriter.convertToDependentGroup(syncDmlList, onlyCurrentIndex);

        List<CompletableFuture<?>> futures = asyncRunDependent(maxTimestamp, joinUpdateSize,
                bulkRequestList,
                dependentGroup.getMainTableJoinDependentList(), dependentGroup.getSlaveTableDependentList());

        try {
            // nested type ： main table change
            List<Dependent> mainTableDependentList = dependentGroup.getMainTableDependentList();
            List<Dependent> filterMainTableDependentList = onlyFieldName == null ?
                    mainTableDependentList : onlyFieldName.isEmpty() ? Collections.emptyList() : mainTableDependentList.stream().filter(e -> e.containsObjectField(onlyFieldName)).collect(Collectors.toList());
            nestedFieldWriter.writeMainTable(filterMainTableDependentList, bulkRequestList, maxIdIn);
        } finally {
            log.info("sync(dml[{}]).BasicFieldWriter={}ms, NestedFieldWriter={}ms, {}, table={}",
                    syncDmlList.size(),
                    basicFieldWriterCost,
                    System.currentTimeMillis() - timestamp,
                    maxTimestamp, tables);
            cacheMap.cacheClear();
            bulkRequestList.commit(esTemplate);
        }

        // call listeners
        if (!listenerList.isEmpty()) {
            try {
                invokeAllListener(syncDmlList, bulkRequestList);
            } catch (InterruptedException | ExecutionException e) {
                Util.sneakyThrows(e);
            } finally {
                cacheMap.cacheClear();
                bulkRequestList.commit(esTemplate); // 批次统一提交
            }
        }

        if (futures.isEmpty()) {
            future.complete(null);
        } else {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            future.completeExceptionally(throwable);
                        } else {
                            future.complete(null);
                            if (refresh) {
                                esTemplate.refresh(allIndices);
                            }
                        }
                    });
        }

        // refresh
        if (refresh && dmls.size() < refreshThreshold) {
            esTemplate.refresh(indices);
        }
        return future;
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

    public boolean isAutoUpdateChildren() {
        return autoUpdateChildren;
    }

    public void setAutoUpdateChildren(boolean autoUpdateChildren) {
        this.autoUpdateChildren = autoUpdateChildren;
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

}
