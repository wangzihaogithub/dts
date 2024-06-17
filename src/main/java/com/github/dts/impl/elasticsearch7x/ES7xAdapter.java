package com.github.dts.impl.elasticsearch7x;

import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


/**
 * ES外部适配器
 */
public class ES7xAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(ES7xAdapter.class);
    private final Map<String, ESSyncConfig> esSyncConfig = new ConcurrentHashMap<>(); // 文件名对应配置
    private final Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置
    private final List<ESSyncServiceListener> listenerList = new ArrayList<>();
    private final CacheMap cacheMap = new CacheMap();
    private ES7xConnection esConnection;
    private CanalConfig.OuterAdapterConfig configuration;
    private ESTemplate esTemplate;
    private ExecutorService listenerExecutor;
    private BasicFieldWriter basicFieldWriter;
    private NestedFieldWriter nestedFieldWriter;
    @Value("${spring.profiles.active:}")
    private String env;

    private static String getEsSyncConfigKey(String destination, String database, String table) {
        return destination + "_" + database + "_" + table;
    }

    private static Map<String, ESSyncConfig> loadYamlToBean(Properties envProperties, File resourcesDir, String env) {
        log.info("## Start loading es mapping config ... {}", resourcesDir);
        Map<String, ESSyncConfig> esSyncConfig = new LinkedHashMap<>();
        Map<String, byte[]> yamlMap = loadYamlToBytes(resourcesDir);
        for (Map.Entry<String, byte[]> entry : yamlMap.entrySet()) {
            String fileName = entry.getKey();
            byte[] content = entry.getValue();
            ESSyncConfig config = YmlConfigBinder.bindYmlToObj(null, content, ESSyncConfig.class, envProperties);
            if (config == null) {
                continue;
            }
            if (!Objects.equals(env, config.getEsMapping().getEnv())) {
                continue;
            }
            try {
                config.init();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e, e);
            }
            esSyncConfig.put(fileName, config);
        }
        log.info("## ES mapping config loaded");
        return esSyncConfig;
    }

    private static Map<String, byte[]> loadYamlToBytes(File configDir) {
        Map<String, byte[]> map = new LinkedHashMap<>();
        // 先取本地文件，再取类路径
        File[] files = configDir.listFiles();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (!fileName.endsWith(".yml") && !fileName.endsWith(".yaml")) {
                    continue;
                }
                try {
                    byte[] bytes = Files.readAllBytes(file.toPath());
                    map.put(fileName, bytes);
                } catch (IOException e) {
                    throw new RuntimeException("Read " + configDir + "mapping config: " + fileName + " error. ", e);
                }
            }
        }
        return map;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig(String database, String table) {
        return dbTableEsSyncConfig.get(getEsSyncConfigKey(getDestination(), database, table));
    }

    @Override
    public void init(CanalConfig.OuterAdapterConfig configuration, Properties envProperties) {
        this.configuration = configuration;
        this.esConnection = new ES7xConnection(configuration.getEs7x());
        this.esTemplate = new ES7xTemplate(esConnection);
        this.basicFieldWriter = new BasicFieldWriter(esTemplate);
        this.listenerExecutor = Util.newFixedThreadPool(
                50,
                60_000L, "ES-listener", false);
        loadESSyncConfig(dbTableEsSyncConfig, esSyncConfig, envProperties, configuration.getEs7x().resourcesDir(), env);

        this.listenerList.sort(AnnotationAwareOrderComparator.INSTANCE);
        this.listenerList.forEach(item -> item.init(esSyncConfig));
        this.nestedFieldWriter = new NestedFieldWriter(configuration.getEs7x().getNestedFieldThreads(), esSyncConfig, esTemplate, cacheMap);
    }

    @Override
    public void sync(List<Dml> dmls) {
        sync(dmls, true);
    }

    public void sync(List<Dml> dmls, boolean refresh) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        ESTemplate.BulkRequestList bulkRequestList = esTemplate.newBulkRequestList();
        List<Dml> syncDmlList = dmls.stream().filter(e -> !Boolean.TRUE.equals(e.getIsDdl())).collect(Collectors.toList());
        String tables = syncDmlList.stream().map(Dml::getTable).distinct().collect(Collectors.joining(","));
        Dml max = syncDmlList.stream().filter(e -> e.getEs() != null).max(Comparator.comparing(Dml::getEs)).orElse(null);
        Timestamp maxTimestamp = max == null ? null : new Timestamp(max.getEs());

        Set<String> indices = new LinkedHashSet<>();
        long timstamp = System.currentTimeMillis();
        long basicFieldWriterCost;
        try {
            Map<Map<String, ESSyncConfig>, List<Dml>> groupByMap = syncDmlList.stream()
                    .collect(Collectors.groupingBy(dml -> {
                        String key = getEsSyncConfigKey(dml.getDestination(), dml.getDatabase(), dml.getTable());
                        return Objects.requireNonNull(dbTableEsSyncConfig.get(key), "Miss ESSyncConfig " + key);
                    }));
            for (Map.Entry<Map<String, ESSyncConfig>, List<Dml>> entry : groupByMap.entrySet()) {
                for (ESSyncConfig value : entry.getKey().values()) {
                    indices.add(value.getEsMapping().get_index());
                }
                basicFieldWriter.write(entry.getKey().values(), entry.getValue(), bulkRequestList);
            }
        } finally {
            basicFieldWriterCost = System.currentTimeMillis() - timstamp;
            cacheMap.cacheClear();
            commit(bulkRequestList);

            // refresh
            if (refresh) {
                esTemplate.refresh(indices);
            }
        }

        timstamp = System.currentTimeMillis();
        try {
            nestedFieldWriter.write(syncDmlList, bulkRequestList);
        } finally {
            log.info("sync(dml[{}]).BasicFieldWriter={}ms, NestedFieldWriter={}ms, {}, table={}",
                    syncDmlList.size(),
                    basicFieldWriterCost,
                    System.currentTimeMillis() - timstamp,
                    maxTimestamp, tables);
            cacheMap.cacheClear();
            commit(bulkRequestList);
        }

        // call listeners
        if (!listenerList.isEmpty()) {
            try {
                invokeAllListener(syncDmlList, bulkRequestList);
            } catch (InterruptedException | ExecutionException e) {
                Util.sneakyThrows(e);
            } finally {
                cacheMap.cacheClear();
                commit(bulkRequestList); // 批次统一提交
            }
        }

        // refresh
        if (refresh && dmls.size() < 10) {
            esTemplate.refresh(indices);
        }
    }

    private void commit(ESTemplate.BulkRequestList bulkRequestList) {
        esTemplate.commit();
        if (bulkRequestList != null && !bulkRequestList.isEmpty()) {
            esTemplate.bulk(bulkRequestList);
            esTemplate.commit();
        }
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
        Watch watch = new Watch();
        watch.start("es7.call.listeners(). size=" + listenerList.size());
        for (Future<Object> future : futures) {
            future.get();
        }
        watch.stop();
    }

    private void loadESSyncConfig(Map<String, Map<String, ESSyncConfig>> map,
                                  Map<String, ESSyncConfig> configMap,
                                  Properties envProperties, File resourcesDir, String env) {
        Map<String, ESSyncConfig> load = loadYamlToBean(envProperties, resourcesDir, env);
        for (Map.Entry<String, ESSyncConfig> entry : load.entrySet()) {
            ESSyncConfig config = entry.getValue();
            if (!config.getEsMapping().isEnable()) {
                continue;
            }
            String configName = entry.getKey();
            configMap.put(configName, config);
            String schema = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());

            for (SchemaItem.TableItem item : config.getEsMapping().getSchemaItem().getAliasTableItems().values()) {
                map.computeIfAbsent(getEsSyncConfigKey(config.getDestination(), schema, item.getTableName()),
                        k -> new ConcurrentHashMap<>()).put(configName, config);
            }
            for (Map.Entry<String, ESSyncConfig.ObjectField> e : config.getEsMapping().getObjFields().entrySet()) {
                ESSyncConfig.ObjectField v = e.getValue();
                if (v.getSchemaItem() == null || CollectionUtils.isEmpty(v.getSchemaItem().getAliasTableItems())) {
                    continue;
                }
                for (SchemaItem.TableItem tableItem : v.getSchemaItem().getAliasTableItems().values()) {
                    map.computeIfAbsent(getEsSyncConfigKey(config.getDestination(), schema, tableItem.getTableName()),
                                    k -> new ConcurrentHashMap<>())
                            .put(configName, config);
                }
            }
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

    public ESTemplate getEsTemplate() {
        return esTemplate;
    }
}
