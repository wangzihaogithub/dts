package com.github.dts.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SdkSubscriber {

    private final Map<String, SdkChannels> channelsMap = new ConcurrentHashMap<>();

    public boolean isOpen(String account) {
        SdkChannels sdkChannels = channelsMap.get(account);
        return sdkChannels != null && sdkChannels.isOpen();
    }

    public void flush(String account) {
        SdkChannels sdkChannels = channelsMap.get(account);
        if (sdkChannels != null && sdkChannels.isOpen()) {
            sdkChannels.flush();
        }
    }

    public void add(SdkChannel sdkChannel) {
        get(sdkChannel.getAccount()).add(sdkChannel);
    }

    public SdkChannels get(String sdkAccount) {
        return channelsMap.computeIfAbsent(sdkAccount, e -> new SdkChannels());
    }

    public void write(String sdkAccount, SdkMessage writeMessage) {
        get(sdkAccount).write(writeMessage);
    }

    public void remove(SdkChannel sdkChannel) {
        sdkChannel.close();
        SdkChannels sdkChannels = channelsMap.get(sdkChannel.getAccount());
        if (sdkChannels != null) {
            sdkChannels.remove(sdkChannel);
        }
    }

    public static class SdkChannels {
        private static final Logger log = LoggerFactory.getLogger(SdkChannels.class);
        private final List<SdkChannel> channelList = new CopyOnWriteArrayList<>();

        public void add(SdkChannel channel) {
            channelList.add(channel);
        }

        public boolean remove(SdkChannel channel) {
            return channelList.remove(channel);
        }

        public boolean isOpen() {
            for (SdkChannel channel : channelList) {
                if (channel.isOpen()) {
                    return true;
                }
            }
            return false;
        }

        public void write(SdkMessage writeMessage) {
            for (SdkChannel channel : channelList) {
                channel.write(writeMessage);
            }
        }

        public void flush() {
            for (SdkChannel channel : channelList) {
                try {
                    channel.flush();
                } catch (IOException e) {
                    log.warn("failed to flush sdk channel {} {}", channel.getAccount(), e.toString());
                }
            }
        }
    }

}
