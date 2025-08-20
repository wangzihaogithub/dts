package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class AbstractMessageService {
    public static final int DINGTALK_MAX_CONTENT_LENGTH = 15000;
    private static final Logger log = LoggerFactory.getLogger(AbstractMessageService.class);
    private final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    private final RestTemplate restTemplate = new RestTemplate(requestFactory);
    private final ScheduledExecutorService retryScheduled = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName(AbstractMessageService.this.getClass().getSimpleName() + ".retry-" + thread.getId());
            return thread;
        }
    });

    public static class LogMessageService extends AbstractMessageService {
        private static final Logger log = LoggerFactory.getLogger(LogMessageService.class);
        public static final LogMessageService INSTANCE = new LogMessageService();

        @Override
        public Map send(String title, String content) {
            log.warn("messageService {}: {}", title, content);
            return Collections.emptyMap();
        }
    }

    @Value("${spring.profiles.active:}")
    private String env;
    private int maxRetryCount = 10;

    {
        requestFactory.setConnectTimeout(1000);
        requestFactory.setReadTimeout(3000);
    }

    private static String sign(String secret, long timestamp) {
        try {
            String stringToSign = timestamp + "\n" + secret;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"));
            byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
            String sign = URLEncoder.encode(new String(Base64.getEncoder().encode(signData)), "UTF-8");
            return sign;
        } catch (Throwable t) {
            Util.sneakyThrows(t);
            return null;
        }
    }

    public abstract Map send(String title, String content);

    public Map sendDingtalk(String title1, String content1, String token, String secret) {
        return sendDingtalk(title1, content1, token, secret, maxRetryCount);
    }

    public Map sendDingtalk(String title1, String content1, String token, String secret, int retryCount) {
        String title = title1 + "(" + Util.getIPAddressPort() + " 环境:" + env + ")";

        String content = content1 != null && content1.length() > DINGTALK_MAX_CONTENT_LENGTH ? content1.substring(0, DINGTALK_MAX_CONTENT_LENGTH) : content1;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json; charset=utf-8");

        Map<String, Object> markdown = new HashMap<>();
        markdown.put("title", title);
        content = title + "\n\n" + content;
        markdown.put("text", content);

        Map<String, Object> body = new HashMap<>();
        body.put("msgtype", "markdown");
        body.put("markdown", markdown);

        long timeMillis = System.currentTimeMillis();

        String url = String.format("https://oapi.dingtalk.com/robot/send?access_token=%s&timestamp=%s&sign=%s",
                token,
                timeMillis,
                sign(secret, timeMillis)
        );
        try {
            Map map = restTemplate.postForObject(url, new HttpEntity<>(body, headers), Map.class);
            return map;
        } catch (Exception e) {
            if (retryCount > 0) {
                retryScheduled.schedule(() -> sendDingtalk(title1, content1, token, secret, retryCount - 1), 60, TimeUnit.SECONDS);
            }
            log.warn("sendDingtalk {} error {}, retryCount: {}", title, e.toString(), retryCount);
            return Collections.emptyMap();
        }
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }
}
