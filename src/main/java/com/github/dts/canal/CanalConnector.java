package com.github.dts.canal;

import com.github.dts.util.Dml;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface CanalConnector {
    void connect();

    void subscribe(String[] topic);

    void rollback();

    void ack();

    void ack2();

    List<Dml> getListWithoutAck(Duration timeout);

    void disconnect();

    void setPullSize(Integer pullSize);

    Map<String, Object> setDiscard(boolean discard) throws InterruptedException;

    default void rebuildConsumer(Consumer<CanalConnector> rebuildConsumer) {

    }

    default void close() {

    }
}
