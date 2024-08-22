package com.github.dts.cluster;

import java.io.Flushable;
import java.io.IOException;
import java.nio.channels.Channel;
import java.security.Principal;

public interface SdkChannel extends Channel, Flushable {

    Principal getPrincipal();

    default String getAccount() {
        return getPrincipal().getName();
    }

    @Override
    boolean isOpen();

    void write(SdkMessage writeMessage);

    void flush() throws IOException;

    void close();
}