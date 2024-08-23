package com.github.dts.cluster;

import com.github.dts.util.KeepaliveSocket;

import java.io.IOException;
import java.util.List;

public class SdkInstance {
    private String ip;
    private Integer port;
    private String deviceId;
    private String account;
    private String password;

    public static boolean isSocketConnected(SdkInstance instance, int timeout) {
        try (KeepaliveSocket socket = new KeepaliveSocket(instance.getIp(), instance.getPort())) {
            if (socket.isConnected(timeout)) {
                return true;
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}