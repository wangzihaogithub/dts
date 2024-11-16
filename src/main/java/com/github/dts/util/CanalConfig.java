package com.github.dts.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.dts.canal.CanalConnector;
import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.rds.RDSAdapter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.sql.DataSource;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * canal 的相关配置类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@ConfigurationProperties(prefix = "canal.conf")
public class CanalConfig {
    private final ClusterConfig cluster = new ClusterConfig();
    private Map<String, DatasourceConfig> srcDataSources;
    private boolean enablePull = true;
    // canal adapters 配置
    private List<CanalAdapter> canalAdapters;

    public ClusterConfig getCluster() {
        return cluster;
    }

    public boolean isEnablePull() {
        return enablePull;
    }

    public void setEnablePull(boolean enablePull) {
        this.enablePull = enablePull;
    }

    public Map<String, DatasourceConfig> getSrcDataSources() {
        return srcDataSources;
    }

    public void setSrcDataSources(Map<String, DatasourceConfig> srcDataSources) {
        this.srcDataSources = srcDataSources;
        if (srcDataSources != null) {
            DatasourceConfig.setDataSource(srcDataSources);
        }
    }

    public List<CanalAdapter> getCanalAdapters() {
        return canalAdapters;
    }

    public void setCanalAdapters(List<CanalAdapter> canalAdapters) {
        this.canalAdapters = canalAdapters;
    }

    @Override
    public String toString() {
        return "CanalClientConfig{" +
                "canalAdapters=" + canalAdapters +
                '}';
    }

    public enum DiscoveryEnum {
        AUTO,
        REDIS,
        NACOS,
        DISABLE
    }

    public static class DatasourceConfig {

        private final static Map<String, DataSource> DATA_SOURCES = new ConcurrentHashMap<>(); // key对应的数据源

        private String driver = "com.mysql.cj.jdbc.Driver";   // 默认为mysql jdbc驱动
        private String url;                                      // jdbc url
        private String database;                                 // jdbc database
        private String type = "mysql";                   // 类型, 默认为mysql
        private String username;                                 // jdbc username
        private String password;                                 // jdbc password
        private Integer maxActive = 100;                         // 连接池最大连接数,默认为3
        private Integer maxWait = 120000;
        private boolean lazy = true;

        public static boolean contains(DataSource dataSource) {
            return DATA_SOURCES.containsValue(dataSource);
        }

        public static DataSource getDataSource(String key) {
            return DATA_SOURCES.get(key);
        }

        public static String getDataSourceKey0() {
            if (DATA_SOURCES.size() == 1) {
                return DATA_SOURCES.keySet().iterator().next();
            } else {
                return null;
            }
        }

        public static String getCatalog(String dataSourceKey) {
            DataSource dataSource = getDataSource(dataSourceKey);
            if (dataSource instanceof DruidDataSource) {
                return getCatalogByUrl(((DruidDataSource) dataSource).getUrl());
            } else {
                throw new IllegalStateException("miss datasource key: " + dataSourceKey);
            }
        }

        public static String getAddressByUrl(String url) throws URISyntaxException {
            if (url == null || url.isEmpty()) {
                return url;
            }
            if (url.startsWith("jdbc:")) {
                url = url.substring("jdbc:".length());
            }
            URI uri = new URI(url);
            return uri.getRawAuthority();
        }

        public static String getCatalogByUrl(String url) {
            Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
            Matcher matcher = pattern.matcher(url);
            if (!matcher.find()) {
                throw new RuntimeException("Not found the schema of jdbc-url: " + url);
            }
            String schema = matcher.group(2);
            return schema;
        }

        public static void close() {
            for (DataSource druidDataSource : DATA_SOURCES.values()) {
                try {
                    if (druidDataSource instanceof DruidDataSource) {
                        ((DruidDataSource) druidDataSource).close();
                    }
                } catch (Exception ignored) {
                }
            }
            DATA_SOURCES.clear();
        }

        private static void setDataSource(Map<String, DatasourceConfig> srcDataSources) {
            for (Map.Entry<String, DatasourceConfig> entry : srcDataSources.entrySet()) {
                DatasourceConfig.DATA_SOURCES.put(entry.getKey(), druidDataSource(entry.getValue()));
            }
        }

        private static DruidDataSource druidDataSource(DatasourceConfig config) {
            // 加载数据源连接池
            DruidDataSource ds = new DruidDataSource();
            ds.setDriverClassName(config.getDriver());
            ds.setUrl(config.getUrl());
            ds.setUsername(config.getUsername());
            ds.setPassword(config.getPassword());
            ds.setInitialSize(1);
            ds.setMinIdle(1);
            ds.setMaxActive(config.getMaxActive());
            ds.setMaxWait(config.getMaxWait());
            ds.setTimeBetweenEvictionRunsMillis(60000);
            ds.setMinEvictableIdleTimeMillis(300000);
            ds.setValidationQuery("select 1");
            if (!config.isLazy()) {
                try {
                    ds.init();
                } catch (SQLException e) {
                    Util.sneakyThrows(e);
                }
            }
            return ds;
        }

        public boolean isLazy() {
            return lazy;
        }

        public void setLazy(boolean lazy) {
            this.lazy = lazy;
        }

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Integer getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Integer maxWait) {
            this.maxWait = maxWait;
        }

        public Integer getMaxActive() {
            return maxActive;
        }

        public void setMaxActive(Integer maxActive) {
            this.maxActive = maxActive;
        }
    }

    public static class SdkAccount {
        private String account;
        private String password;

        public void validate() {
            if (account == null || account.isEmpty()) {
                throw new IllegalArgumentException("account is empty");
            }
            if (password == null || password.isEmpty()) {
                throw new IllegalArgumentException("password is empty");
            }
        }

        public String getAccount() {
            return account;
        }

        public void setAccount(String account) {
            this.account = account;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class ClusterConfig {
        private final Redis redis = new Redis();
        private final Nacos nacos = new Nacos();
        private int testSocketTimeoutMs = 500;
        private int httpWriteCommitSize = 100;

        private DiscoveryEnum discovery = DiscoveryEnum.AUTO;
        private String groupName = "def";
        private List<SdkAccount> sdkAccount;

        public int getHttpWriteCommitSize() {
            return httpWriteCommitSize;
        }

        public void setHttpWriteCommitSize(int httpWriteCommitSize) {
            this.httpWriteCommitSize = httpWriteCommitSize;
        }

        public List<SdkAccount> getSdkAccount() {
            return sdkAccount;
        }

        public void setSdkAccount(List<SdkAccount> sdkAccount) {
            this.sdkAccount = sdkAccount;
        }

        public int getTestSocketTimeoutMs() {
            return testSocketTimeoutMs;
        }

        public void setTestSocketTimeoutMs(int testSocketTimeoutMs) {
            this.testSocketTimeoutMs = testSocketTimeoutMs;
        }

        public Nacos getNacos() {
            return nacos;
        }

        public DiscoveryEnum getDiscovery() {
            return discovery;
        }

        public void setDiscovery(DiscoveryEnum discovery) {
            this.discovery = discovery;
        }

        public String getGroupName() {
            return groupName;
        }

        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        public Redis getRedis() {
            return redis;
        }

        public static class Redis {
            private String redisConnectionFactoryBeanName = "redisConnectionFactory";
            private String redisKeyRootPrefix = "dts:${spring.profiles.active:def}";
            private int redisInstanceExpireSec = 10;
            private int messageIdIncrementDelta = 50;
            // 防止sub，pub命令有延迟，增加定时轮训
            private int updateInstanceTimerMs = 5000;

            public int getUpdateInstanceTimerMs() {
                return updateInstanceTimerMs;
            }

            public void setUpdateInstanceTimerMs(int updateInstanceTimerMs) {
                this.updateInstanceTimerMs = updateInstanceTimerMs;
            }

            public int getMessageIdIncrementDelta() {
                return messageIdIncrementDelta;
            }

            public void setMessageIdIncrementDelta(int messageIdIncrementDelta) {
                this.messageIdIncrementDelta = messageIdIncrementDelta;
            }

            public String getRedisConnectionFactoryBeanName() {
                return redisConnectionFactoryBeanName;
            }

            public void setRedisConnectionFactoryBeanName(String redisConnectionFactoryBeanName) {
                this.redisConnectionFactoryBeanName = redisConnectionFactoryBeanName;
            }

            public String getRedisKeyRootPrefix() {
                return redisKeyRootPrefix;
            }

            public void setRedisKeyRootPrefix(String redisKeyRootPrefix) {
                this.redisKeyRootPrefix = redisKeyRootPrefix;
            }

            public int getRedisInstanceExpireSec() {
                return redisInstanceExpireSec;
            }

            public void setRedisInstanceExpireSec(int redisInstanceExpireSec) {
                this.redisInstanceExpireSec = redisInstanceExpireSec;
            }

        }

        public static class Nacos {
            private String serverAddr = "${nacos.discovery.server-addr:${nacos.config.server-addr:${spring.cloud.nacos.server-addr:${spring.cloud.nacos.discovery.server-addr:${spring.cloud.nacos.config.server-addr:}}}}}";
            private String namespace = "${nacos.discovery.namespace:${nacos.config.namespace:${spring.cloud.nacos.namespace:${spring.cloud.nacos.discovery.namespace:${spring.cloud.nacos.config.namespace:}}}}}";
            private String serviceName = "${spring.application.name:dts}";
            private String clusterName = "${nacos.discovery.clusterName:${nacos.config.clusterName:${spring.cloud.nacos.clusterName:${spring.cloud.nacos.discovery.clusterName:${spring.cloud.nacos.config.clusterName:DEFAULT}}}}}";
            private Properties properties = new Properties();

            public Properties buildProperties() {
                Properties properties = new Properties();
                properties.putAll(this.properties);
                if (serverAddr != null && !serverAddr.isEmpty()) {
                    properties.put("serverAddr", serverAddr);
                }
                if (namespace != null && !namespace.isEmpty()) {
                    properties.put("namespace", namespace);
                }
                return properties;
            }

            public String getServerAddr() {
                return serverAddr;
            }

            public void setServerAddr(String serverAddr) {
                this.serverAddr = serverAddr;
            }

            public String getNamespace() {
                return namespace;
            }

            public void setNamespace(String namespace) {
                this.namespace = namespace;
            }

            public String getServiceName() {
                return serviceName;
            }

            public void setServiceName(String serviceName) {
                this.serviceName = serviceName;
            }

            public String getClusterName() {
                return clusterName;
            }

            public void setClusterName(String clusterName) {
                this.clusterName = clusterName;
            }

            public Properties getProperties() {
                return properties;
            }

            public void setProperties(Properties properties) {
                this.properties = properties;
            }
        }

    }

    public static class CanalAdapter {
        private boolean enable = true;
        private Class<? extends CanalConnector> connector = com.github.dts.canal.MysqlBinlogCanalConnector.class;
        private String clientIdentity;
        private String[] destination = new String[]{"defaultDestination"}; // 实例名
        private String[] topics; // mq topics
        private List<Group> groups;  // 适配器分组列表
        private Properties properties = new Properties();
        // 批大小
        private Integer batchSize = 500;
        // 毫秒时间内拉取
        private Integer pullTimeout = 5;
        // redis- meta数据前缀
        private String redisMetaPrefix = "dts:${spring.profiles.active:def}";

        public String getRedisMetaPrefix() {
            return redisMetaPrefix;
        }

        public void setRedisMetaPrefix(String redisMetaPrefix) {
            this.redisMetaPrefix = redisMetaPrefix;
        }

        public Class<? extends CanalConnector> getConnector() {
            return connector;
        }

        public void setConnector(Class<? extends CanalConnector> connector) {
            this.connector = connector;
        }

        public CanalConnector newCanalConnector(CanalConfig canalConfig, StartupServer startupServer, boolean rebuild) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
            Constructor<? extends CanalConnector> constructor = connector.getDeclaredConstructor(CanalConfig.class, CanalAdapter.class, StartupServer.class, boolean.class);
            constructor.setAccessible(true);
            return constructor.newInstance(canalConfig, this, startupServer, rebuild);
        }

        public String getClientIdentity() {
            return clientIdentity;
        }

        public void setClientIdentity(String clientIdentity) {
            this.clientIdentity = clientIdentity;
        }

        public String clientIdentity() {
            if (clientIdentity != null && !clientIdentity.isEmpty()) {
                return clientIdentity;
            } else if (destination != null && destination.length > 0) {
                return String.join(",", destination);
            } else {
                return null;
            }
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public Integer getPullTimeout() {
            return pullTimeout;
        }

        public void setPullTimeout(Integer pullTimeout) {
            this.pullTimeout = pullTimeout;
        }

        public boolean isEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        @Override
        public String toString() {
            return "CanalAdapter{" +
                    "destination='" + Arrays.toString(destination) + '\'' +
                    ", groups=" + groups +
                    '}';
        }

        public String[] getTopics() {
            return topics;
        }

        public void setTopics(String[] topics) {
            this.topics = topics;
        }

        public String[] getDestination() {
            return destination;
        }

        public void setDestination(String[] destination) {
            this.destination = destination;
        }

        public Properties getProperties() {
            return properties;
        }

        public void setProperties(Properties properties) {
            this.properties = properties;
        }

        public List<Group> getGroups() {
            return groups;
        }

        public void setGroups(List<Group> groups) {
            this.groups = groups;
        }
    }

    public static class OuterAdapterConfig {
        private final Es es = new Es();
        private final Rds rds = new Rds();
        private Group connectorGroup;
        private CanalAdapter canalAdapter;
        private String name;       // 适配器名称, 如: logger, hbase, es

        public Class<? extends Adapter> adapterClass() {
            if (es.getAddress() != null && es.getAddress().length > 0) {
                return ESAdapter.class;
            } else {
                return RDSAdapter.class;
            }
        }

        @Deprecated
        public Es getEs7x() {
            return es;
        }

        public Es getEs() {
            return es;
        }

        public Rds getRds() {
            return rds;
        }

        @Override
        public String toString() {
            return "OuterAdapterConfig{" +
                    "name='" + name + '\'' +
                    '}';
        }

        public CanalAdapter getCanalAdapter() {
            return canalAdapter;
        }

        public void setCanalAdapter(CanalAdapter canalAdapter) {
            this.canalAdapter = canalAdapter;
        }

        public Group getConnectorGroup() {
            return connectorGroup;
        }

        public void setConnectorGroup(Group connectorGroup) {
            this.connectorGroup = connectorGroup;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public static class Rds {

        }

        public static class EsAccount {
            private String[] address;// es 读地址
            private String clusterName;
            private String username;// 账号，来源：租户账号
            private String password;// 密码，来源：租户密码
            private String apiKey;
            private int updateByQueryChunkSize = 1000;
            private int maxRetryCount = 0;// 错误请求重试几次
            private int bulkRetryCount = 0;
            private int concurrentBulkRequest = 16;// 最大并发bulk请求
            private int minAvailableSpaceHighBulkRequests = 2;// 高优先级bulk最少可用空间数量，最大实时性越好，保证实时性
            private int bulkCommitSize = 200;//每次bulk请求的大约提交条数
            private int httpKeepAliveMinutes = 3000;
            private int httpConnectTimeout = 10 * 60 * 60;
            private int httpRequestTimeout = 100 * 60 * 60;
            private int httpSocketTimeout = 100 * 60 * 60;

            public String[] getAddress() {
                return address;
            }

            public void setAddress(String[] address) {
                this.address = address;
            }

            public String getClusterName() {
                return clusterName;
            }

            public void setClusterName(String clusterName) {
                this.clusterName = clusterName;
            }

            public String getUsername() {
                return username;
            }

            public void setUsername(String username) {
                this.username = username;
            }

            public String getPassword() {
                return password;
            }

            public void setPassword(String password) {
                this.password = password;
            }

            public String getApiKey() {
                return apiKey;
            }

            public void setApiKey(String apiKey) {
                this.apiKey = apiKey;
            }

            public int getUpdateByQueryChunkSize() {
                return updateByQueryChunkSize;
            }

            public void setUpdateByQueryChunkSize(int updateByQueryChunkSize) {
                this.updateByQueryChunkSize = updateByQueryChunkSize;
            }

            public int getMaxRetryCount() {
                return maxRetryCount;
            }

            public void setMaxRetryCount(int maxRetryCount) {
                this.maxRetryCount = maxRetryCount;
            }

            public int getBulkRetryCount() {
                return bulkRetryCount;
            }

            public void setBulkRetryCount(int bulkRetryCount) {
                this.bulkRetryCount = bulkRetryCount;
            }

            public int getConcurrentBulkRequest() {
                return concurrentBulkRequest;
            }

            public void setConcurrentBulkRequest(int concurrentBulkRequest) {
                this.concurrentBulkRequest = concurrentBulkRequest;
            }

            public int getMinAvailableSpaceHighBulkRequests() {
                return minAvailableSpaceHighBulkRequests;
            }

            public void setMinAvailableSpaceHighBulkRequests(int minAvailableSpaceHighBulkRequests) {
                this.minAvailableSpaceHighBulkRequests = minAvailableSpaceHighBulkRequests;
            }

            public int getBulkCommitSize() {
                return bulkCommitSize;
            }

            public void setBulkCommitSize(int bulkCommitSize) {
                this.bulkCommitSize = bulkCommitSize;
            }

            public int getHttpKeepAliveMinutes() {
                return httpKeepAliveMinutes;
            }

            public void setHttpKeepAliveMinutes(int httpKeepAliveMinutes) {
                this.httpKeepAliveMinutes = httpKeepAliveMinutes;
            }

            public int getHttpConnectTimeout() {
                return httpConnectTimeout;
            }

            public void setHttpConnectTimeout(int httpConnectTimeout) {
                this.httpConnectTimeout = httpConnectTimeout;
            }

            public int getHttpRequestTimeout() {
                return httpRequestTimeout;
            }

            public void setHttpRequestTimeout(int httpRequestTimeout) {
                this.httpRequestTimeout = httpRequestTimeout;
            }

            public int getHttpSocketTimeout() {
                return httpSocketTimeout;
            }

            public void setHttpSocketTimeout(int httpSocketTimeout) {
                this.httpSocketTimeout = httpSocketTimeout;
            }
        }

        public static class Es extends EsAccount {
            private final SlaveNestedField slaveNestedField = new SlaveNestedField();
            private final MainJoinNestedField mainJoinNestedField = new MainJoinNestedField();
            private String resourcesDir = "es";
            private int refreshThreshold = 10;
            private int listenerThreads = 50;
            private int maxQueryCacheSize = 10000;//查询缓存大小
            private boolean shareAdapterCache = true; // Adapter是否共享一个缓存
            private int nestedFieldThreads = 10;
            private int joinUpdateSize = 10;
            private int streamChunkSize = 10000;
            private int maxIdIn = 1000;
            private boolean refresh = true;
            private int commitEventPublishScheduledTickMs = 100;
            private int commitEventPublishMaxBlockCount = 50000;
            private boolean onlyEffect = true;// 只更新受到影响的字段

            public boolean isRefresh() {
                return refresh;
            }

            public void setRefresh(boolean refresh) {
                this.refresh = refresh;
            }

            public boolean isShareAdapterCache() {
                return shareAdapterCache;
            }

            public void setShareAdapterCache(boolean shareAdapterCache) {
                this.shareAdapterCache = shareAdapterCache;
            }

            public boolean isOnlyEffect() {
                return onlyEffect;
            }

            public void setOnlyEffect(boolean onlyEffect) {
                this.onlyEffect = onlyEffect;
            }

            public int getCommitEventPublishMaxBlockCount() {
                return commitEventPublishMaxBlockCount;
            }

            public void setCommitEventPublishMaxBlockCount(int commitEventPublishMaxBlockCount) {
                this.commitEventPublishMaxBlockCount = commitEventPublishMaxBlockCount;
            }

            public int getCommitEventPublishScheduledTickMs() {
                return commitEventPublishScheduledTickMs;
            }

            public void setCommitEventPublishScheduledTickMs(int commitEventPublishScheduledTickMs) {
                this.commitEventPublishScheduledTickMs = commitEventPublishScheduledTickMs;
            }

            public int getMaxIdIn() {
                return maxIdIn;
            }

            public void setMaxIdIn(int maxIdIn) {
                this.maxIdIn = maxIdIn;
            }

            public MainJoinNestedField getMainJoinNestedField() {
                return mainJoinNestedField;
            }

            public int getStreamChunkSize() {
                return streamChunkSize;
            }

            public void setStreamChunkSize(int streamChunkSize) {
                this.streamChunkSize = streamChunkSize;
            }

            public int getJoinUpdateSize() {
                return joinUpdateSize;
            }

            public void setJoinUpdateSize(int joinUpdateSize) {
                this.joinUpdateSize = joinUpdateSize;
            }

            public int getNestedFieldThreads() {
                return nestedFieldThreads;
            }

            public void setNestedFieldThreads(int nestedFieldThreads) {
                this.nestedFieldThreads = nestedFieldThreads;
            }

            public int getMaxQueryCacheSize() {
                return maxQueryCacheSize;
            }

            public void setMaxQueryCacheSize(int maxQueryCacheSize) {
                this.maxQueryCacheSize = maxQueryCacheSize;
            }

            public int getListenerThreads() {
                return listenerThreads;
            }

            public void setListenerThreads(int listenerThreads) {
                this.listenerThreads = listenerThreads;
            }

            public int getRefreshThreshold() {
                return refreshThreshold;
            }

            public void setRefreshThreshold(int refreshThreshold) {
                this.refreshThreshold = refreshThreshold;
            }

            public String getResourcesDir() {
                return resourcesDir;
            }

            public void setResourcesDir(String resourcesDir) {
                this.resourcesDir = resourcesDir;
            }

            public File resourcesDir() {
                return Util.getConfDirPath(resourcesDir);
            }

            public SlaveNestedField getSlaveNestedField() {
                return slaveNestedField;
            }

            public static class MainJoinNestedField {
                private int threads = 1;
                private int queues = 1000;
                private boolean block = false;// 写从表是否阻塞主表

                public int getThreads() {
                    return threads;
                }

                public void setThreads(int threads) {
                    this.threads = threads;
                }

                public int getQueues() {
                    return queues;
                }

                public void setQueues(int queues) {
                    this.queues = queues;
                }

                public boolean isBlock() {
                    return block;
                }

                public void setBlock(boolean block) {
                    this.block = block;
                }
            }

            public static class SlaveNestedField {
                private int threads = 1;
                private int queues = 1000;
                private boolean block = false;// 写从表是否阻塞主表

                public int getThreads() {
                    return threads;
                }

                public void setThreads(int threads) {
                    this.threads = threads;
                }

                public int getQueues() {
                    return queues;
                }

                public void setQueues(int queues) {
                    this.queues = queues;
                }

                public boolean isBlock() {
                    return block;
                }

                public void setBlock(boolean block) {
                    this.block = block;
                }
            }
        }

    }

    public static class Group {

        private List<OuterAdapterConfig> outerAdapters;                           // 适配器列表
        private Map<String, OuterAdapterConfig> outerAdaptersMap = new LinkedHashMap<>();

        @Override
        public String toString() {
            return "Group{" +
                    "outerAdapters=" + outerAdapters +
                    ", outerAdaptersMap=" + outerAdaptersMap +
                    '}';
        }

        public List<OuterAdapterConfig> getOuterAdapters() {
            return outerAdapters;
        }

        public void setOuterAdapters(List<OuterAdapterConfig> outerAdapters) {
            this.outerAdapters = outerAdapters;
        }

        public Map<String, OuterAdapterConfig> getOuterAdaptersMap() {
            return outerAdaptersMap;
        }

        public void setOuterAdaptersMap(Map<String, OuterAdapterConfig> outerAdaptersMap) {
            this.outerAdaptersMap = outerAdaptersMap;
        }
    }

}
