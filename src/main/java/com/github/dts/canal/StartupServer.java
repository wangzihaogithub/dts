package com.github.dts.canal;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.impl.rds.RDSAdapter;
import com.github.dts.util.Adapter;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.MessageServiceImpl;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Order(Integer.MIN_VALUE)
public class StartupServer implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(StartupServer.class);
    private final Map<String, List<ThreadRef>> canalThreadMap = new HashMap<>();
    private final Map<String, Adapter> adapterMap = new ConcurrentHashMap<>();
    @Autowired
    private BeanFactory beanFactory;
    @Autowired
    private MessageServiceImpl messageService;
    @Resource
    private CanalConfig canalConfig;
    private volatile boolean running = false;

    public List<ThreadRef> getCanalThread(String destination) {
        List<ThreadRef> threadRef = canalThreadMap.get(destination);
        return threadRef != null ? threadRef : Collections.emptyList();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (running) {
            return;
        }
        try {
            log.info("## start the canal client adapters.");
            start(canalConfig);
            running = true;
            log.info("## the canal client adapters are running now ......");
        } catch (Exception e) {
            log.error("## something goes wrong when starting up the canal client adapters:", e);
            throw e;
        }
    }

    public <T extends Adapter> T getAdapter(String name, Class<T> type) {
        return type.cast(adapterMap.get(name));
    }

    /**
     * 初始化canal-client
     * (用spring容器替代SPI 2019年4月29日 15:31:23)
     */
    public void start(CanalConfig canalClientConfig) {
        // 初始化canal-client的适配器
        if (canalClientConfig.getCanalAdapters() == null) {
            log.info("adapter for canal is empty config");
            return;
        }
        for (CanalConfig.CanalAdapter canalAdapter : canalClientConfig.getCanalAdapters()) {
            if (!canalAdapter.isEnable()) {
                continue;
            }
            List<Adapter> adapterList = new ArrayList<>();
            for (CanalConfig.Group connectorGroup : canalAdapter.getGroups()) {
                for (CanalConfig.OuterAdapterConfig config : connectorGroup.getOuterAdapters()) {
                    config.setCanalAdapter(canalAdapter);
                    config.setConnectorGroup(connectorGroup);
                    adapterList.add(loadAdapter(config));
                }
            }

            ThreadRef thread = new ThreadRef(canalClientConfig,
                    canalAdapter, adapterList, messageService);
            canalThreadMap.computeIfAbsent(canalAdapter.getDestination(), e -> new ArrayList<>())
                    .add(thread);
            log.info("Start adapter for canal destination: {} succeed", canalAdapter.getDestination());
        }
    }

    private Adapter loadAdapter(CanalConfig.OuterAdapterConfig config) {
        try {
            Environment env = beanFactory.getBean(Environment.class);
            Properties evnProperties = null;
            if (env instanceof StandardEnvironment) {
                evnProperties = new Properties();
                for (org.springframework.core.env.PropertySource<?> propertySource : ((StandardEnvironment) env).getPropertySources()) {
                    if (propertySource instanceof org.springframework.core.env.EnumerablePropertySource) {
                        String[] names = ((EnumerablePropertySource<?>) propertySource).getPropertyNames();
                        for (String name : names) {
                            Object val = propertySource.getProperty(name);
                            if (val != null) {
                                evnProperties.put(name, val);
                            }
                        }
                    }
                }
            }

            String name = config.getName();
            Adapter adapter = adapterMap.get(name);
            if (adapter == null) {
                if (config.getEs7x() != null) {
                    adapter = beanFactory.getBean(ES7xAdapter.class);
                } else if (config.getRds() != null) {
                    adapter = beanFactory.getBean(RDSAdapter.class);
                } else {
                    throw new IllegalArgumentException("adapter");
                }
                adapterMap.put(name, adapter);
                adapter.init(config, evnProperties);
                log.info("Load canal adapter: {} succeed", config.getName());
            }
            return adapter;
        } catch (Exception e) {
            log.error("Load canal adapter: {} failed", config.getName(), e);
            throw e;
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        if (!running) {
            return;
        }
        try {
            running = false;
            log.info("## stop the canal client adapters");
            if (!canalThreadMap.isEmpty()) {
                ExecutorService service = Executors.newFixedThreadPool(canalThreadMap.size());
                for (List<ThreadRef> canalAdapterWorker : canalThreadMap.values()) {
                    for (ThreadRef threadRef : canalAdapterWorker) {
                        service.execute(threadRef.canalThread::destroy0);
                    }
                }
                service.shutdown();
                try {
                    while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                        // ignore
                    }
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            log.info("All canal adapters destroyed");
            for (DataSource druidDataSource : CanalConfig.DatasourceConfig.DATA_SOURCES.values()) {
                try {
                    if (druidDataSource instanceof DruidDataSource) {
                        ((DruidDataSource) druidDataSource).close();
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            CanalConfig.DatasourceConfig.DATA_SOURCES.clear();
        } catch (Throwable e) {
            log.warn("## something goes wrong when stopping canal client adapters:", e);
        } finally {
            log.info("## canal client adapters are down.");
        }
    }

    public static class ThreadRef {
        private final CanalConfig canalConfig;
        private final CanalConfig.CanalAdapter config;
        private final List<Adapter> adapterList;
        private final MessageServiceImpl messageService;
        private CanalThread canalThread;

        public ThreadRef(CanalConfig canalConfig, CanalConfig.CanalAdapter config,
                         List<Adapter> adapterList, MessageServiceImpl messageService) {
            this.canalConfig = canalConfig;
            this.config = config;
            this.adapterList = adapterList;
            this.messageService = messageService;
            startThread();
        }

        @SneakyThrows
        public void startThread() {
            CanalConnector canalConnector = config.newCanalConnector(canalConfig);
            canalThread = new CanalThread(canalConfig, config, adapterList, messageService,
                    canalConnector,
                    t -> {
                        t.setRunning(false);
                        new Thread(ThreadRef.this::startThread).start();
                    });
            if (canalConfig.isEnablePull()) {
                canalThread.start();
            }
        }

        public void stopThread() {
            canalThread.setRunning(false);
        }

        public CanalThread getCanalThread() {
            return canalThread;
        }
    }
}
