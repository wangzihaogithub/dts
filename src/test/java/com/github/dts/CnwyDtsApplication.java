package com.github.dts;

import com.github.dts.util.CanalConfig;
import com.github.dts.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties(CanalConfig.class)
@SpringBootApplication
public class CnwyDtsApplication {
    private static final Logger log = LoggerFactory.getLogger(CnwyDtsApplication.class);

    /**
     * 环境隔离
     */
    private static String ENV;
    private static volatile long startUp;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SpringApplication app = new SpringApplication(CnwyDtsApplication.class);
        app.setLogStartupInfo(false);
        app.run(args);
        log.info("start up [CanalAdapterApplication] Application Startup Success, time used:{} ms",
                System.currentTimeMillis() - start);
        startUp = System.currentTimeMillis();
    }

    public static boolean isStartup() {
        return startUp > 0L;
    }

    public static String getENV() {
        return ENV;
    }

    @Autowired
    public void setEnv(@Value("${spring.profiles.active:}") String env,
                       @Value("${server.port:8080}") Integer port) {
        ENV = env;
        Util.port = port;
    }
}
