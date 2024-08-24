package com.github.dts.conf;

import com.github.dts.cluster.SdkLoginService;
import com.github.dts.util.CanalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.*;
import java.util.stream.Collectors;

public class ConfigSdkLoginService implements SdkLoginService {
    private static final Logger log = LoggerFactory.getLogger(ConfigSdkLoginService.class);
    private final Map<String, List<CanalConfig.SdkAccount>> sdkAccountMap;

    public ConfigSdkLoginService(CanalConfig canalConfig) {
        List<CanalConfig.SdkAccount> sdkAccount = canalConfig.getCluster().getSdkAccount();
        if (sdkAccount == null) {
            sdkAccount = Collections.emptyList();
        }
        for (CanalConfig.SdkAccount account : sdkAccount) {
            account.validate();
        }
        this.sdkAccountMap = sdkAccount.stream().collect(Collectors.groupingBy(CanalConfig.SdkAccount::getAccount));
    }

    @Override
    public Principal loginSdk(String authorization) {
        if (sdkAccountMap.isEmpty()) {
            return null;
        }
        if (authorization == null || !authorization.startsWith("Basic ")) {
            log.info("loginSdk fail by authorization is '{}'", authorization);
            return null;
        }
        String token = authorization.substring("Basic ".length());
        String[] accountAndPassword;
        try {
            accountAndPassword = new String(Base64.getDecoder().decode(token)).split(":", 2);
        } catch (Exception e) {
            log.info("loginSdk fail by base64 error {}, {}", authorization, e.toString());
            return null;
        }
        if (accountAndPassword.length != 2) {
            log.info("loginSdk fail by accountAndPassword.length != 2 {}", authorization);
            return null;
        }
        String account = accountAndPassword[0];
        String password = accountAndPassword[1];
        List<CanalConfig.SdkAccount> sdkAccountList = sdkAccountMap.get(account);
        if (sdkAccountList == null || sdkAccountList.isEmpty()) {
            log.info("loginSdk fail by account is not exist. account={}", account);
            return null;
        }
        for (CanalConfig.SdkAccount sdkAccount : sdkAccountList) {
            String dbPassword = sdkAccount.getPassword();
            if (Objects.equals(dbPassword, password)) {
                log.info("loginSdk success. account={}", account);
                return () -> account;
            }
        }
        log.info("loginSdk fail by password error. account={}", account);
        return null;
    }

    @Override
    public Principal fetchSdk(String authorization) {
        return loginSdk(authorization);
    }
}
