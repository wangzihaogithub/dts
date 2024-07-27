package com.github.dts.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.dts.canal.CanalConnector;
import com.github.dts.canal.StartupServer;
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
    private Map<String, DatasourceConfig> srcDataSources;
    private boolean enablePull = true;
    // canal adapters 配置
    private List<CanalAdapter> canalAdapters;

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
            for (Map.Entry<String, DatasourceConfig> entry : srcDataSources.entrySet()) {
                DatasourceConfig.DATA_SOURCES.put(entry.getKey(), druidDataSource(entry.getValue()));
            }
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

    private DruidDataSource druidDataSource(DatasourceConfig datasourceConfig) {
        // 加载数据源连接池
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName(datasourceConfig.getDriver());
        ds.setUrl(datasourceConfig.getUrl());
        ds.setUsername(datasourceConfig.getUsername());
        ds.setPassword(datasourceConfig.getPassword());
        ds.setInitialSize(1);
        ds.setMinIdle(1);
        ds.setMaxActive(datasourceConfig.getMaxActive());
        ds.setMaxWait(60000);
        ds.setTimeBetweenEvictionRunsMillis(60000);
        ds.setMinEvictableIdleTimeMillis(300000);
        ds.setValidationQuery("select 1");
        if (!datasourceConfig.isLazy()) {
            try {
                ds.init();
            } catch (SQLException e) {
                Util.sneakyThrows(e);
            }
        }
        return ds;
    }

    public static class DatasourceConfig {

        public final static Map<String, DataSource> DATA_SOURCES = new ConcurrentHashMap<>(); // key对应的数据源

        private String driver = "com.mysql.cj.jdbc.Driver";   // 默认为mysql jdbc驱动
        private String url;                                      // jdbc url
        private String database;                                 // jdbc database
        private String type = "mysql";                   // 类型, 默认为mysql
        private String username;                                 // jdbc username
        private String password;                                 // jdbc password
        private Integer maxActive = 100;                         // 连接池最大连接数,默认为3
        private boolean lazy = true;

        public void setLazy(boolean lazy) {
            this.lazy = lazy;
        }

        public boolean isLazy() {
            return lazy;
        }

        public static DataSource getDataSource(String key) {
            return DATA_SOURCES.get(key);
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

        public Integer getMaxActive() {
            return maxActive;
        }

        public void setMaxActive(Integer maxActive) {
            this.maxActive = maxActive;
        }
    }

    public static class CanalAdapter {
        private boolean enable = true;
        private Class<? extends CanalConnector> connector;
        private String clientIdentity;
        private String[] destination; // 实例名
        private String[] topics; // mq topics
        private List<Group> groups;  // 适配器分组列表
        private Properties properties = new Properties();
        // 批大小
        private Integer batchSize = 500;
        // 毫秒时间内拉取
        private Integer pullTimeout = 5;
        // redis- meta数据前缀
        private String redisMetaPrefix = "dts:${spring.profiles.active:}";

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
        private final Es7x es7x = new Es7x();
        private final Rds rds = new Rds();
        private Group connectorGroup;
        private CanalAdapter canalAdapter;
        private String name;       // 适配器名称, 如: logger, hbase, es

        public Es7x getEs7x() {
            return es7x;
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

        public static class Es7x {
            private final SlaveNestedField slaveNestedField = new SlaveNestedField();
            private final MainJoinNestedField mainJoinNestedField = new MainJoinNestedField();
            private String resourcesDir = "es";
            private String[] address;// es 读地址
            private String username;// 账号，来源：租户账号
            private String password;// 密码，来源：租户密码
            private Map<String, String> properties; // 其余参数, 可填写适配器中的所需的配置信息
            private int maxRetryCount = 1;// 错误请求重试几次
            private int bulkRetryCount = 1;
            private int concurrentBulkRequest = 16;// 最大并发bulk请求
            private int minAvailableSpaceHighBulkRequests = 2;// 高优先级bulk最少可用空间数量，最大实时性越好，保证实时性
            private int bulkCommitSize = 200;//每次bulk请求的大约提交条数
            private boolean refresh = true;
            private int refreshThreshold = 10;
            private int listenerThreads = 50;
            private int maxQueryCacheSize = 10000;//查询缓存大小
            private int nestedFieldThreads = 10;
            private int joinUpdateSize = 10;
            private int streamChunkSize = 1000;
            private int basicMaxIdIn = 500;

            public int getMinAvailableSpaceHighBulkRequests() {
                return minAvailableSpaceHighBulkRequests;
            }

            public void setMinAvailableSpaceHighBulkRequests(int minAvailableSpaceHighBulkRequests) {
                this.minAvailableSpaceHighBulkRequests = minAvailableSpaceHighBulkRequests;
            }

            public int getBulkRetryCount() {
                return bulkRetryCount;
            }

            public void setBulkRetryCount(int bulkRetryCount) {
                this.bulkRetryCount = bulkRetryCount;
            }

            public int getBasicMaxIdIn() {
                return basicMaxIdIn;
            }

            public void setBasicMaxIdIn(int basicMaxIdIn) {
                this.basicMaxIdIn = basicMaxIdIn;
            }

            public MainJoinNestedField getMainJoinTableField() {
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

            public boolean isRefresh() {
                return refresh;
            }

            public void setRefresh(boolean refresh) {
                this.refresh = refresh;
            }

            public int getMaxRetryCount() {
                return maxRetryCount;
            }

            public void setMaxRetryCount(int maxRetryCount) {
                this.maxRetryCount = maxRetryCount;
            }

            public int getBulkCommitSize() {
                return bulkCommitSize;
            }

            public void setBulkCommitSize(int bulkCommitSize) {
                this.bulkCommitSize = bulkCommitSize;
            }

            public int getConcurrentBulkRequest() {
                return concurrentBulkRequest;
            }

            public void setConcurrentBulkRequest(int concurrentBulkRequest) {
                this.concurrentBulkRequest = concurrentBulkRequest;
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

            public String[] getAddress() {
                return address;
            }

            public void setAddress(String[] address) {
                this.address = address;
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

            public Map<String, String> getProperties() {
                return properties;
            }

            public void setProperties(Map<String, String> properties) {
                this.properties = properties;
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
