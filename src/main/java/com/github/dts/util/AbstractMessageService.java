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

public abstract class AbstractMessageService {
    public static final int DINGTALK_MAX_CONTENT_LENGTH = 15000;
    private static final Logger log = LoggerFactory.getLogger(AbstractMessageService.class);
    private final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    private final RestTemplate restTemplate = new RestTemplate(requestFactory);
    @Value("${spring.profiles.active:}")
    private String env;

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

    public Map sendDingtalk(String title, String content, String token, String secret) {
        title = title + "(" + Util.getIPAddressPort() + " " + env + ")";

        content = content != null && content.length() > DINGTALK_MAX_CONTENT_LENGTH ? content.substring(0, DINGTALK_MAX_CONTENT_LENGTH) : content;
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
            log.warn("sendDingtalk {} error {}", title, e.toString());
            return Collections.emptyMap();
        }
    }
}
