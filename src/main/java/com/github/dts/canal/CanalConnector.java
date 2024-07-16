package com.github.dts.canal;

import com.github.dts.util.Dml;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface CanalConnector {
    Ack2 NULL_ACK = new Ack2() {
        @Override
        public void ack() {

        }

        @Override
        public String toString() {
            return "NULL_ACK";
        }
    };

    void connect();

    void subscribe(String[] topic);

    void rollback();

    void ack();

    List<Dml> getListWithoutAck(Duration timeout);

    default Ack2 getAck2() {
        return NULL_ACK;
    }

    void disconnect();

    void setPullSize(Integer pullSize);

    Map<String, Object> setDiscard(boolean discard) throws InterruptedException;

    default void rebuildConsumer(Consumer<CanalConnector> rebuildConsumer) {

    }

    default void close() {

    }

    interface Ack2 {
        void ack();
    }
}
