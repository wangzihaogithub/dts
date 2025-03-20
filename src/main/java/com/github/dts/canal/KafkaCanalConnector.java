package com.github.dts.canal;

import com.github.dts.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class KafkaCanalConnector implements CanalConnector {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Object seekLock = new Object();
    private final Properties properties;
    private final Map<TopicPartition, Offset> currentOffsets = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile boolean connected = false;
    private KafkaConsumer<String, Dml> kafkaConsumer;
    private Integer pullSize;
    private String groupId;
    private String[] lastTopic;
    private Properties lastProperties;
    private volatile boolean rebuilding = false;
    private volatile boolean discard = false;
    private volatile Map<String, Object> discardResult;
    private Consumer<CanalConnector> rebuildConsumer;

    public KafkaCanalConnector(CanalConfig canalConfig, CanalConfig.CanalAdapter config,
                               StartupServer startupServer, boolean rebuild) {
        this(config.getProperties());
    }

    public KafkaCanalConnector(Properties config) {
        Properties properties = new Properties();
        properties.put("enable.auto.commit", "false");
        // earliest
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        // latest
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        // none
        //topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        properties.put("auto.offset.reset", "earliest"); // 如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", "600000"); // 10分钟 必须大于session.timeout.ms的设置
        properties.put("session.timeout.ms", "300000"); // 5分钟
        properties.put("isolation.level", "read_committed"); // read_uncommitted
        properties.put("max.poll.interval.ms", "300000"); // 5分钟
        properties.put("fetch.max.bytes", 10 * 1024 * 1024); // 10M
        properties.putAll(config);
        String location = properties.getProperty(org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (location != null && !location.isEmpty()) {
            properties.setProperty(org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getClass().getResource(location).getPath());
        }
        this.properties = properties;
    }

    @Override
    public Map<String, Object> setDiscard(boolean discard) throws InterruptedException {
        this.discard = discard;
        if (lastTopic != null && connected && discard) {
            while (this.discard) {
                Thread.sleep(100);
            }
            return discardResult;
        }
        return null;
    }

    /**
     * 打开连接
     */
    @Override
    public void connect() {
        Properties properties = new Properties();
        properties.putAll(this.properties);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        if (!properties.containsKey("value.deserializer")) {
            properties.put("value.deserializer", DmlDeserializer.class.getName());
        }
        if (pullSize != null) {
            properties.put("max.poll.records", String.valueOf(pullSize));
        }
        connect(properties);
    }

    private void connect(Properties properties) {
        if (connected) {
            return;
        }
        connected = true;
        kafkaConsumer = new KafkaConsumer<>(properties);
        this.groupId = properties.getProperty("group.id");
        this.lastProperties = properties;
    }

    public String getGroupId() {
        return groupId;
    }

    /**
     * 关闭链接
     */
    @Override
    public void disconnect() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            kafkaConsumer = null;
        }
        connected = false;
    }

    @Override
    public void rebuildConsumer(Consumer<CanalConnector> rebuildConsumer) {
        this.rebuildConsumer = rebuildConsumer;
    }

    private boolean rebuild(int timeout) throws InterruptedException {
        if (rebuildConsumer != null) {
            rebuildConsumer.accept(this);
            return true;
        }
        try {
            kafkaConsumer.unsubscribe();
        } catch (Exception e) {
            logger.warn("rebuild unsubscribe error = {}", e.toString(), e);
        }
        try {
            disconnect();
        } catch (Exception e) {
            logger.warn("rebuild disconnect error = {}", e.toString(), e);
        }
        connect(lastProperties);
        subscribe(lastTopic);
        try {
            lock.lock();
            rebuilding = true;
            return condition.await(timeout, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 订阅topic
     */
    @Override
    public void subscribe(String[] topic) {
        kafkaConsumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                rollback(partitions);
                if (rebuilding) {
                    try {
                        lock.lock();
                        condition.signalAll();
                    } finally {
                        lock.unlock();
                        rebuilding = false;
                    }
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                rollback(partitions);
                if (rebuilding) {
                    try {
                        lock.lock();
                        condition.signalAll();
                    } finally {
                        lock.unlock();
                        rebuilding = false;
                    }
                }
            }
        });
        this.lastTopic = topic;
    }

    public Map<String, Object> discard() {
        synchronized (seekLock) {
            Set<TopicPartition> partitions = kafkaConsumer.assignment();
            Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                TopicPartition partition = entry.getKey();
                Long offset = entry.getValue();
                logger.info("discard[{}] seek last committed offset => {}", partition, offset);
                kafkaConsumer.seek(partition, offset);
            }

            getAck2().ack();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("endOffsets", endOffsets.toString());
            result.put("currentOffsets", currentOffsets.toString());
            return result;
        }
    }

    @Override
    public void rollback() {
        rollback(kafkaConsumer.assignment());
    }

    public void rollback(Collection<TopicPartition> partitions) {
        synchronized (seekLock) {
            for (TopicPartition topicPartition : partitions) {
                OffsetAndMetadata offset = kafkaConsumer.committed(topicPartition);
                if (offset == null || offset.offset() <= 0L) {
                    offset = new OffsetAndMetadata(kafkaConsumer.position(topicPartition));
                    logger.info("rollback[{}] seek to end offset => {}", topicPartition, offset.offset());
                } else {
                    logger.info("rollback[{}] seek last committed offset => {}", topicPartition, offset.offset());
                }
                kafkaConsumer.seek(topicPartition, offset);
            }
        }
    }

    public void putProperties(Map properties) {
        this.properties.putAll(properties);
    }

    @Override
    public void setPullSize(Integer pullSize) {
        this.pullSize = pullSize;
    }

    @Override
    public List<Dml> getListWithoutAck(Duration timeout) {
        if (kafkaConsumer == null) {
            return Collections.emptyList();
        }
        if (discard) {
            this.discardResult = discard();
            discard = false;
        }
        ConsumerRecords<String, Dml> partitionRecords = kafkaConsumer.poll(timeout);
        currentOffsets.clear();
        if (partitionRecords.isEmpty()) {
            return Collections.emptyList();
        }

        List<Dml> dmlList = new LinkedList<>();
        int index = 0;
        String packetId = UUID.randomUUID().toString();
        try {
            for (TopicPartition partition : partitionRecords.partitions()) {
                List<ConsumerRecord<String, Dml>> records = partitionRecords.records(partition);
                if (records.isEmpty()) {
                    continue;
                }

                ConsumerRecord<String, Dml> min = records.stream().min(Comparator.comparing(ConsumerRecord::offset)).orElse(null);
                ConsumerRecord<String, Dml> max = records.stream().max(Comparator.comparing(ConsumerRecord::offset)).orElse(null);

                for (ConsumerRecord<String, Dml> record : records) {
                    Dml dml = record.value();
                    dml.setIndex(index++);
                    dml.setPacketId(packetId);
                    Long ts = dml.getTs();
                    if (ts == null || ts == 0L) {
                        dml.setTs(record.timestamp());
                    }
                    dml.setGroupId(groupId);
                    dml.setLogfileOffset(record.offset());
                    dml.setEventLength(record.serializedValueSize());
                    dml.setLogfileName(partition.toString());
                    dml.setDestination(record.topic());
                    dmlList.add(dml);
                }

                Offset offset = new Offset(partition, min, max);
                currentOffsets.put(partition, offset);
            }
        } finally {
            seekUndoPos();
        }
        Dml.compress(dmlList);
        dmlList.sort(Comparator.comparing(Dml::getEs));
        return new ArrayList<>(dmlList);
    }

    public void seekUndoPos() {
        synchronized (seekLock) {
            for (Map.Entry<TopicPartition, Offset> entry : currentOffsets.entrySet()) {
                Offset value = entry.getValue();
                kafkaConsumer.seek(entry.getKey(), new OffsetAndMetadata(value.min.offset(), value.min.leaderEpoch(), null));
            }
        }
    }

    public void commit() {
        if (currentOffsets.isEmpty()) {
            return;
        }
        synchronized (seekLock) {
            Map<TopicPartition, OffsetAndMetadata> commit = new LinkedHashMap<>();
            for (Map.Entry<TopicPartition, Offset> entry : currentOffsets.entrySet()) {
                Offset value = entry.getValue();
                commit.put(entry.getKey(), new OffsetAndMetadata(value.max.offset() + 1, value.max.leaderEpoch(), null));
            }
            try {
                kafkaConsumer.commitSync(commit);
            } catch (CommitFailedException e) {
                try {
                    rebuild(60_000);
                } catch (InterruptedException ex) {
                    throw e;
                }
                kafkaConsumer.commitSync(commit);
            }

            for (Map.Entry<TopicPartition, Offset> entry : currentOffsets.entrySet()) {
                Offset value = entry.getValue();
                kafkaConsumer.seek(entry.getKey(), new OffsetAndMetadata(value.max.offset() + 1, value.max.leaderEpoch(), null));
            }
        }
    }

    /**
     * 提交offset，如果超过 session.timeout.ms 设置的时间没有ack则会抛出异常，ack失败
     */
    @Override
    public void ack() {
        commit();
    }

    public static class Offset {
        private final TopicPartition topicPartition;
        private final ConsumerRecord<String, Dml> min;
        private final ConsumerRecord<String, Dml> max;

        public Offset(TopicPartition topicPartition,
                      ConsumerRecord<String, Dml> min, ConsumerRecord<String, Dml> max) {
            this.topicPartition = topicPartition;
            this.min = min;
            this.max = max;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public long getMinOffset() {
            return min.offset();
        }

        public long getMaxOffset() {
            return max.offset();
        }
    }

    public static class DmlDeserializer implements Deserializer<Dml> {
        private static final Charset UTF_8 = Charset.forName("UTF-8");
        private static final JsonUtil.ObjectReader OBJECT_READER = JsonUtil.objectReader();

        private static void castType(Dml dml, List<Map<String, Object>> list) {
            Map<String, String> mysqlType = dml.getMysqlType();
            if (mysqlType == null || list == null) {
                return;
            }
            for (Map<String, Object> data : list) {
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String key = entry.getKey();
                    String type = mysqlType.get(key);
                    if (type == null) {
                        continue;
                    }
                    switch (type) {
                        case "bit": {
                            Object value = entry.getValue();
                            if (value != null && !"".equals(value)) {
                                entry.setValue(Byte.valueOf(value.toString()));
                            }
                            break;
                        }
                        case "bigint": {
                            Object value = entry.getValue();
                            if (value != null && !"".equals(value)) {
                                entry.setValue(Long.valueOf(value.toString()));
                            }
                            break;
                        }
                        case "int": {
                            Object value = entry.getValue();
                            if (value != null && !"".equals(value)) {
                                entry.setValue(Integer.valueOf(value.toString()));
                            }
                            break;
                        }
                        case "timestamp":
                        case "date":
                        case "datetime": {
                            Object value = entry.getValue();
                            if (value != null && !"".equals(value)) {
                                entry.setValue(DateUtil.parseDate(value.toString()));
                            }
                            break;
                        }

                    }
                }
            }
        }

        private static void removeEqualsOld(List<Map<String, Object>> dataList, List<Map<String, Object>> oldList) {
            if (dataList == null || oldList == null || dataList.size() != oldList.size()) {
                return;
            }
            int size = dataList.size();
            for (int i = 0; i < size; i++) {
                Map<String, Object> data = dataList.get(i);
                Map<String, Object> old = oldList.get(i);
                List<String> removeKeyList = new ArrayList<>();
                for (Map.Entry<String, Object> entry : old.entrySet()) {
                    String key = entry.getKey();
                    Object oldValue = entry.getValue();
                    Object dataValue = data.get(key);
                    if (Objects.equals(oldValue, dataValue)) {
                        removeKeyList.add(key);
                    }
                }
                for (String removeKey : removeKeyList) {
                    old.remove(removeKey);
                }
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public Dml deserialize(String topic, Headers headers, byte[] data) {
            return deserialize(topic, data);
        }

        @Override
        public Dml deserialize(String topic1, byte[] payload) {
            Dml dml = null;
            try {
                dml = OBJECT_READER.readValue(payload, Dml.class);
            } catch (IOException e) {
                Util.sneakyThrows(e);
            }

            List<Map<String, Object>> dataList = dml.getData();
            List<Map<String, Object>> oldList = dml.getOld();
            removeEqualsOld(dataList, oldList);
            castType(dml, dataList);
            castType(dml, oldList);

            return dml;
        }

        @Override
        public void close() {
        }
    }

}
