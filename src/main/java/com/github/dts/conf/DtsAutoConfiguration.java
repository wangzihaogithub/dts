package com.github.dts.conf;

import com.github.dts.canal.StartupServer;
import com.github.dts.cluster.DiscoveryService;
import com.github.dts.cluster.SdkSubscriber;
import com.github.dts.cluster.SdkSubscriberHttpServlet;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.rds.RDSAdapter;
import com.github.dts.util.AbstractMessageService;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.PlaceholdersResolver;
import com.github.dts.util.Util;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@EnableConfigurationProperties(CanalConfig.class)
@Configuration
public class DtsAutoConfiguration {
    @Bean
    public StartupServer startupServer() {
        return new StartupServer();
    }

    @Bean
    public DiscoveryService discoveryService(CanalConfig canalConfig, SdkSubscriber sdkSubscriber, ListableBeanFactory beanFactory) {
        return DiscoveryService.newInstance(canalConfig.getCluster(), sdkSubscriber, beanFactory);
    }

    @Bean
    @ConditionalOnClass(name = {"org.springframework.boot.web.servlet.ServletRegistrationBean", "javax.servlet.Servlet"})
    public ServletRegistrationBean<SdkSubscriberHttpServlet> clusterServlet(CanalConfig canalConfig,
                                                                            SdkSubscriber sdkSubscriber,
                                                                            @Autowired(required = false) DiscoveryService discoveryService
    ) {
        ServletRegistrationBean<SdkSubscriberHttpServlet> registrationBean = new ServletRegistrationBean<>();
        registrationBean.setServlet(new SdkSubscriberHttpServlet(sdkSubscriber, canalConfig, discoveryService));
        registrationBean.addUrlMappings("/dts/sdk/subscriber");
        registrationBean.setAsyncSupported(true);
        return registrationBean;
    }

    @Bean
    public SdkSubscriber sdkSubscriber() {
        return new SdkSubscriber();
    }

    @Bean
    @Scope("prototype")
    public ESAdapter esAdapter() {
        return new ESAdapter();
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
        return AbstractMessageService.LogMessageService.INSTANCE;
    }

    @Autowired
    public void setEnv(@Value("${server.port:8080}") Integer port) {
        Util.port = port;
    }

}
