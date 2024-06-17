package com.github.dts.conf;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.impl.rds.RDSAdapter;
import com.github.dts.util.AbstractMessageService;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.IGPlaceholdersResolver;
import com.github.dts.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
    public IGPlaceholdersResolver placeholdersResolver() {
        return new IGPlaceholdersResolver();
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

}
