package com.github.dts.canal;

import com.github.dts.cluster.DiscoveryService;
import com.github.dts.util.AbstractMessageService;
import com.github.dts.util.Adapter;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Order(Integer.MIN_VALUE)
public class StartupServer implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(StartupServer.class);
    private final Map<String, List<ThreadRef>> canalThreadMap = Util.newLinkedCaseInsensitiveMap();
    private final Map<String, Adapter> adapterMap = Util.newLinkedCaseInsensitiveMap();
    @Autowired
    private ListableBeanFactory beanFactory;
    @Autowired
    private AbstractMessageService messageService;
    @Resource
    private CanalConfig canalConfig;
    private volatile boolean running = false;
    @Value("${spring.profiles.active:}")
    private String env;
    @Autowired(required = false)
    private DiscoveryService discoveryService;

    public DiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    public String getEnv() {
        return env;
    }

    public BeanFactory getBeanFactory() {
        return beanFactory;
    }

    public AbstractMessageService getMessageService() {
        return messageService;
    }

    public List<ThreadRef> getCanalThread(String clientIdentity) {
        List<ThreadRef> threadRef = canalThreadMap.get(clientIdentity);
        return threadRef != null ? threadRef : Collections.emptyList();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (running) {
            return;
        }
        try {
            log.info("## start '{}' canal client adapters.", env);
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

    public <T extends Adapter> List<T> getAdapter(Class<T> type) {
        List<T> list = new ArrayList<>();
        for (Adapter value : adapterMap.values()) {
            if (type.isAssignableFrom(value.getClass())) {
                list.add((T) value);
            }
        }
        return list;
    }

    public void start(CanalConfig canalConfig) {
        // 初始化canal-client的适配器
        if (canalConfig.getCanalAdapters() == null) {
            log.info("adapter for canal is empty config");
            return;
        }
        Collection<CanalConfig.CanalAdapter> canalAdapterList = canalConfig.getCanalAdapters().stream().filter(CanalConfig.CanalAdapter::isEnable).collect(Collectors.toList());
        if (canalAdapterList.isEmpty()) {
            log.info("adapter for canal is not enable config");
            return;
        }

        Environment env = beanFactory.getBean(Environment.class);
        if (discoveryService != null) {
            discoveryService.registerServerInstance();
        } else {
            log.info("discoveryService is disabled");
        }
        Map<String, AtomicInteger> adapterNameCounter = Util.newLinkedCaseInsensitiveMap();
        for (CanalConfig.CanalAdapter canalAdapter : canalAdapterList) {
            String metaPrefix = canalAdapter.getRedisMetaPrefix();
            if (metaPrefix != null) {
                canalAdapter.setRedisMetaPrefix(env.resolvePlaceholders(metaPrefix));
            }
            List<Adapter> adapterList = new ArrayList<>();
            for (CanalConfig.Group connectorGroup : canalAdapter.getGroups()) {
                for (CanalConfig.OuterAdapterConfig config : connectorGroup.getOuterAdapters()) {
                    config.setCanalAdapter(canalAdapter);
                    config.setConnectorGroup(connectorGroup);
                    adapterList.add(loadAdapter(canalAdapter, config, discoveryService, env, adapterNameCounter));
                }
            }

            String clientIdentity = canalAdapter.clientIdentity();
            ThreadRef thread = new ThreadRef(canalConfig,
                    canalAdapter, adapterList, messageService, this);
            thread.startThread();
            canalThreadMap.computeIfAbsent(clientIdentity, e -> new ArrayList<>())
                    .add(thread);
            log.info("Start adapter for canal clientIdentity: {} succeed", clientIdentity);
        }
    }

    private Adapter loadAdapter(CanalConfig.CanalAdapter canalAdapter,
                                CanalConfig.OuterAdapterConfig config,
                                DiscoveryService discoveryService,
                                Environment env,
                                Map<String, AtomicInteger> adapterNameCounter) {
        try {
            Properties evnProperties = null;
            if (env instanceof StandardEnvironment) {
                evnProperties = new Properties();
                for (org.springframework.core.env.PropertySource<?> propertySource : ((StandardEnvironment) env).getPropertySources()) {
                    if (propertySource instanceof EnumerablePropertySource) {
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
            Class<? extends Adapter> adapterClass = config.adapterClass();
            if (name == null || name.isEmpty()) {
                name = adapterClass.getSimpleName();
                int id = adapterNameCounter.computeIfAbsent(name, k -> new AtomicInteger(-1))
                        .getAndIncrement();
                if (id != -1) {
                    name += id;
                }
                config.setName(name);
            }
            Adapter adapter = adapterMap.get(name);
            if (adapter == null) {
                adapter = beanFactory.getBean(adapterClass);
                adapterMap.put(name, adapter);
                adapter.init(canalAdapter, config, evnProperties, discoveryService);
                log.info("Load canal adapter: {} succeed", config.getName());
            }
            return adapter;
        } catch (Exception e) {
            log.error("Load canal adapter: {} failed", config.getName(), e);
            destroy();
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
            CanalConfig.DatasourceConfig.close();
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
        private final AbstractMessageService messageService;
        private final StartupServer startupServer;
        private CanalThread canalThread;

        public ThreadRef(CanalConfig canalConfig, CanalConfig.CanalAdapter config,
                         List<Adapter> adapterList, AbstractMessageService messageService,
                         StartupServer startupServer) {
            this.startupServer = startupServer;
            this.canalConfig = canalConfig;
            this.config = config;
            this.adapterList = adapterList;
            this.messageService = messageService;
        }

        public void startThread() {
            try {
                CanalThread parent = this.canalThread;
                CanalThread fork = new CanalThread(canalConfig, config, adapterList, messageService,
                        startupServer,
                        parent,
                        t -> {
                            t.setRunning(false);
                            new Thread(ThreadRef.this::startThread).start();
                        });
                if (canalConfig.isEnablePull()) {
                    fork.start();
                }
                this.canalThread = fork;
            } catch (Throwable t) {
                Util.sneakyThrows(t);
            }
        }

        public void stopThread() {
            canalThread.setRunning(false);
        }

        public CanalThread getCanalThread() {
            return canalThread;
        }

        @Override
        public String toString() {
            return "ThreadRef{" +
                    "canalThread=" + canalThread +
                    '}';
        }
    }
}
