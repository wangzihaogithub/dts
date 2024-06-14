package com.github.dts.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class MessageServiceImpl {
    private final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    private final RestTemplate restTemplate = new RestTemplate(requestFactory);
    @Value("${spring.profiles.active:}")
    private String env;
    {
        requestFactory.setConnectTimeout(1000);
        requestFactory.setReadTimeout(3000);
    }

    @SneakyThrows
    private static String sign(String secret) {
        Long timestamp = System.currentTimeMillis();

        String stringToSign = timestamp + "\n" + secret;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"));
        byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
        String sign = URLEncoder.encode(new String(Base64.encodeBase64(signData)), "UTF-8");
        return sign;
    }

    public static void main(String[] args) {
        MessageServiceImpl service = new MessageServiceImpl();
        Map qwe = service.send("qwe", "bbd\nfdsdfs\nwaeqew");
    }

    public Map send(String title, String content) {
        return send(title, content,
                "0713e564365e0c9fd781c1d6f3ad65b60a2a89e4b08e425736f22c8ad78207e8",
                "SEC452f93eedc12963dd90b6fdfba2da75b5afbb58cafb81bfec041230b4251e229"
        );
    }

    /**
     * 发boss-hi
     *
     * @param title
     * @param content
     * @return
     */
    public Map send(String title, String content, String bossHiId, String secret) {
        title = title + "(" + Util.getIPAddress() + " " + env + ")";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json; charset=utf-8");

        Map<String, Object> markdown = new HashMap<>();
        markdown.put("title", title);
        content = title + "\n\n" + content;
        markdown.put("text", content);

        Map<String, Object> body = new HashMap<>();
        body.put("msgtype", "markdown");
        body.put("markdown", markdown);

        String url = String.format("https://oapi.dingtalk.com/robot/send?access_token=%s&timestamp=%s&sign=%s",
                bossHiId,
                System.currentTimeMillis(),
                sign(secret)
        );
        Map map = restTemplate.postForObject(url, new HttpEntity<>(body, headers), Map.class);
        return map;
    }
}
