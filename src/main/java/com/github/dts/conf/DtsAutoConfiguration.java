package com.github.dts.conf;

import com.github.dts.canal.StartupServer;
import com.github.dts.cluster.DiscoveryService;
import com.github.dts.cluster.SdkSubscriber;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.impl.rds.RDSAdapter;
import com.github.dts.util.AbstractMessageService;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.PlaceholdersResolver;
import com.github.dts.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.Collections;
import java.util.Map;

@EnableConfigurationProperties(CanalConfig.class)
@Configuration
public class DtsAutoConfiguration {
    @Bean
    public StartupServer startupServer() {
        return new StartupServer();
    }

    @ConditionalOnProperty(prefix = "canal.conf.cluster", name = "discovery", havingValue = "DISABLE", matchIfMissing = true)
    @Bean
    public DtsClusterAutoConfiguration clusterAutoConfiguration() {
        return new DtsClusterAutoConfiguration();
    }

    @Bean
    public SdkSubscriber sdkSubscriber() {
        return new SdkSubscriber();
    }

    @Bean
    @Scope("prototype")
    public ES7xAdapter es7xAdapter() {
        return new ES7xAdapter();
    }

    @Bean
    @Scope("prototype")
    public RDSAdapter rdsAdapter() {
        return new RDSAdapter();
    }

    @Bean
    public PlaceholdersResolver placeholdersResolver() {
        return new PlaceholdersResolver();
    }

    @Bean
    @ConditionalOnMissingBean(AbstractMessageService.class)
    public AbstractMessageService messageService() {
        return new AbstractMessageService() {
            private final Logger log = LoggerFactory.getLogger(AbstractMessageService.class);

            @Override
            public Map send(String title, String content) {
                log.warn("messageService {}: {}", title, content);
                return Collections.emptyMap();
            }
        };
    }

    @Autowired
    public void setEnv(@Value("${server.port:8080}") Integer port) {
        Util.port = port;
    }

    @Configuration
    public static class DtsClusterAutoConfiguration {
        @Bean
        public DiscoveryService discoveryService(CanalConfig canalConfig, SdkSubscriber sdkSubscriber, ListableBeanFactory beanFactory) {
            return DiscoveryService.newInstance(canalConfig.getCluster(), sdkSubscriber, beanFactory);
        }

        @Bean
        @ConditionalOnClass(name = "org.springframework.boot.web.servlet.ServletRegistrationBean")
        public DtsServletAutoConfiguration servletAutoConfiguration(DiscoveryService discoveryService) {
            return new DtsServletAutoConfiguration(discoveryService);
        }
    }

}
