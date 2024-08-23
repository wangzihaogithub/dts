package com.github.dts.conf;

import com.github.dts.cluster.DiscoveryService;
import com.github.dts.cluster.SdkSubscriber;
import com.github.dts.cluster.SdkSubscriberHttpServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DtsServletAutoConfiguration {
    private final DiscoveryService discoveryService;

    public DtsServletAutoConfiguration(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Bean
    public ServletRegistrationBean<SdkSubscriberHttpServlet> clusterServlet(SdkSubscriber sdkSubscriber) {
        ServletRegistrationBean<SdkSubscriberHttpServlet> registrationBean = new ServletRegistrationBean<>();
        registrationBean.setServlet(new SdkSubscriberHttpServlet(sdkSubscriber, discoveryService));
        registrationBean.addUrlMappings("/dts/sdk/subscriber");
        return registrationBean;
    }

}
