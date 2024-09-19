package com.github.dts.canal;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.common.alarm.LogAlarmHandler;
import com.alibaba.otter.canal.connector.core.util.JdbcTypeUtil;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.instance.spring.CanalInstanceWithSpring;
import com.alibaba.otter.canal.meta.FileMixedMetaManager;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy;
import com.alibaba.otter.canal.parse.index.FailbackLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.index.MetaLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.github.dts.util.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MysqlBinlogCanalConnector implements CanalConnector {
    private static final Logger log = LoggerFactory.getLogger(MysqlBinlogCanalConnector.class);
    private static final Map<String, CanalInstance> canalInstanceMap = new ConcurrentHashMap<>();
    private final CanalServerWithEmbedded server = CanalServerWithEmbedded.instance();
    private final ClientIdentity identity;
    private final InetSocketAddress address;            // 主库信息
    private final String username;                  // 帐号
    private final String password;                  // 密码
    private final String defaultDatabaseName;
    private final int slaveId;
    private final File dataDir;
    private final int maxDumpThread;
    private final DataSource dataSource;
    private final boolean enableGTID;
    private final String[] destination;
    private final StartupServer startupServer;
    private final String metaPrefix;
    private final boolean selectAck2;
    // 最大Canal事件存储内存
    private final RingBufferSizeMemoryEnum eventStoreMemoryEnum;
    private final String redisConnectionFactoryBeanName;
    private CanalInstanceWithSpring subscribe;
    private Integer pullSize;
    private Message lastMessage;
    private volatile Map<String, Object> discardResult;
    private volatile boolean setDiscard = false;
    private Consumer<CanalConnector> rebuildConsumer;
    private boolean connect;
    private MetaDataFileMixedMetaManager metaManager;

    public MysqlBinlogCanalConnector(CanalConfig canalConfig,
                                     CanalConfig.CanalAdapter config,
                                     StartupServer startupServer,
                                     boolean rebuild) throws URISyntaxException {
        Properties properties = config.getProperties();

        this.selectAck2 = !rebuild;
        this.metaPrefix = config.getRedisMetaPrefix();
        this.startupServer = startupServer;
        String clientIdentityName = config.clientIdentity();
        this.destination = config.getDestination();
        this.identity = new ClientIdentity(clientIdentityName, (short) 1001);
        String dataSource = properties.getProperty("dataSource");
        this.maxDumpThread = Integer.parseInt(properties.getProperty("maxDumpThread", String.valueOf(Integer.MAX_VALUE)));
        if (dataSource != null) {
            CanalConfig.DatasourceConfig datasourceConfig = canalConfig.getSrcDataSources().get(dataSource);
            this.username = datasourceConfig.getUsername();
            this.password = datasourceConfig.getPassword();
            this.defaultDatabaseName = CanalConfig.DatasourceConfig.getCatalogByUrl(datasourceConfig.getUrl());
            this.address = parseAddress(CanalConfig.DatasourceConfig.getAddressByUrl(datasourceConfig.getUrl()));
            this.dataSource = dataSource(datasourceConfig.getUrl(), username, password);
        } else {
            String url = properties.getProperty("url");
            this.username = properties.getProperty("username");
            this.password = properties.getProperty("password");
            this.defaultDatabaseName = properties.getProperty("defaultDatabaseName", CanalConfig.DatasourceConfig.getAddressByUrl(url));
            this.address = parseAddress(CanalConfig.DatasourceConfig.getAddressByUrl(url));
            this.dataSource = dataSource(url, username, password);
        }

        this.redisConnectionFactoryBeanName = properties.getProperty("redisConnectionFactoryBeanName", "redisConnectionFactory");
        // JVM内存的${maxEventStoreMemoryJvmRate}%，分给Canal事件存储用
        int maxEventStoreMemoryJvmRate = Integer.parseInt(properties.getProperty("maxEventStoreMemoryJvmRate", "60%").trim().replace("%", "").trim());
        this.eventStoreMemoryEnum = RingBufferSizeMemoryEnum.getByJvmMaxMemoryRate(maxEventStoreMemoryJvmRate);

        log.info("binlog used max eventStoreMemory = {}, by jvm memory{}%", eventStoreMemoryEnum, maxEventStoreMemoryJvmRate);
        this.enableGTID = "true".equalsIgnoreCase(properties.getProperty("enableGTID", "false"));
        this.slaveId = Integer.parseInt(properties.getProperty("slaveId", String.valueOf(generateUniqueServerId(clientIdentityName, Util.getIPAddressPort()))));
        this.dataDir = new File(properties.getProperty("dataDir", System.getProperty("user.dir")));
        server.setCanalInstanceGenerator(canalInstanceMap::get);
    }

    private static InetSocketAddress parseAddress(String addresses) {
        String[] split = addresses.split(":", 2);
        return new InetSocketAddress(split[0], split.length == 2 ? Integer.parseInt(split[1]) : 3306);
    }

    private static int generateUniqueServerId(String destination, String ip) {
        String[] split = ip.split(":", 2);
        int[] ips = Arrays.stream(split[0].split("\\.")).filter(e -> !e.isEmpty()).mapToInt(e -> Integer.valueOf(e.toString())).toArray();
        int port = split.length == 2 ? Integer.parseInt(split[1]) : 0;

        int salt = (destination != null) ? destination.hashCode() : 0;
        int result = (0x7f & salt + port) << 24;
        for (int element : ips) {
            result = salt * result + element;
        }
        return Math.abs(result);
    }

    private static String mysqlType(String mysqlType) {
        if (mysqlType == null || mysqlType.isEmpty()) {
            return mysqlType;
        }
        int i = mysqlType.indexOf("(");
        if (i == -1) {
            return mysqlType;
        } else {
            return mysqlType.substring(0, i);
        }
    }

    public static List<Dml> convert(Message message, String[] destination) {
        if (message.isRaw()) {
            for (ByteString byteString : message.getRawEntries()) {
                try {
                    message.addEntry(CanalEntry.Entry.parseFrom(byteString));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            message.setRaw(false);
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<Dml> msgs = new ArrayList<>(entries.size());
        int index = 0;
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry,
                        e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            final Dml msg = new Dml();
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setDatabase(entry.getHeader().getSchemaName());
            msg.setTable(entry.getHeader().getTableName());
            msg.setType(eventType.toString());
            msg.setEs(entry.getHeader().getExecuteTime());
            msg.setTs(System.currentTimeMillis());
            msg.setSql(rowChange.getSql());
            msg.setDestination(destination);
            msg.setIndex(index++);
            msg.setPacketId(String.valueOf(message.getId()));
            msg.setEventLength(entry.getHeader().getEventLength());
            msg.setLogfileName(entry.getHeader().getLogfileName());
            msg.setLogfileOffset(entry.getHeader().getLogfileOffset());

            msgs.add(msg);
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                msg.setPkNames(new ArrayList<>());
                int i = 0;
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                            && eventType != CanalEntry.EventType.DELETE) {
                        continue;
                    }

                    Map<String, Object> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;

                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }

                    Map<String, String> mysqlType = new HashMap<>();
                    for (CanalEntry.Column column : columns) {
                        if (i == 0) {
                            if (column.getIsKey()) {
                                msg.getPkNames().add(column.getName());
                            }
                        }
                        if (column.getIsNull()) {
                            row.put(column.getName(), null);
                        } else {
                            row.put(column.getName(),
                                    JdbcTypeUtil.typeConvert(msg.getTable(),
                                            column.getName(),
                                            column.getValue(),
                                            column.getSqlType(),
                                            column.getMysqlType()));
                            mysqlType.put(column.getName(), mysqlType(column.getMysqlType()));
                        }
                        // 获取update为true的字段
                        if (column.getUpdated()) {
                            updateSet.add(column.getName());
                        }
                    }
                    msg.setMysqlType(mysqlType);
                    if (!row.isEmpty()) {
                        data.add(row);
                    }

                    if (eventType == CanalEntry.EventType.UPDATE) {
                        Map<String, Object> rowOld = new LinkedHashMap<>();
                        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                            if (updateSet.contains(column.getName())) {
                                if (column.getIsNull()) {
                                    rowOld.put(column.getName(), null);
                                } else {
                                    rowOld.put(column.getName(), JdbcTypeUtil.typeConvert(msg.getTable(),
                                            column.getName(),
                                            column.getValue(),
                                            column.getSqlType(),
                                            column.getMysqlType()));
                                }
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }

                    i++;
                }
                if (!data.isEmpty()) {
                    msg.setData(data);
                }
                if (!old.isEmpty()) {
                    msg.setOld(old);
                }
            }
        }

        return msgs;
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] childrens = dir.list();
            if (childrens != null) {
                for (String children : childrens) {
                    boolean success = deleteDir(new File(dir, children));
                    if (!success) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }

    private static List<Map<String, Object>> selectShowProcesslist(DataSource ds) throws SQLException {
        try (Connection connection = ds.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW  PROCESSLIST")) {
            List<Map<String, Object>> list = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0, j = 1; i < columnCount; i++, j++) {
                    String columnName = metaData.getColumnLabel(j);
                    map.put(columnName, resultSet.getObject(j));
                }
                list.add(map);
            }
            return list;
        }
    }

    private DataSource dataSource(String url, String username, String password) {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(url);
        ds.setUser(username);
        ds.setPassword(password);
        return ds;
    }

    @Override
    public void rebuildConsumer(Consumer<CanalConnector> rebuildConsumer) {
        this.rebuildConsumer = rebuildConsumer;
    }

    private CanalInstanceWithSpring newInstance(String[] topic) {
        // 默认链接的数据库
        CanalInstanceWithSpring instance
                = new CanalInstanceWithSpring();
        instance.setDestination(identity.getDestination());

        CanalAlarmHandler alarmHandler = new AlarmHandler();

        RdsBinlogEventParserProxy eventParser =
                new RdsBinlogEventParserProxy() {
                    @Override
                    protected void processDumpError(Throwable e) {
                        super.processDumpError(e);
                        String message = e.getMessage();
                        if (message != null && message.contains("errno = 1236")) {
                            rebuild(identity.getDestination(), message);
                        }
                    }
                };

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (availableProcessors < 4) {
            eventParser.setParallel(false);
        } else {
            eventParser.setParallelThreadSize(availableProcessors * 40 / 100); // 40%的能力跑解析,剩余部分处理网络
            eventParser.setParallelBufferSize(256); // 必须为2的幂
        }

        eventParser.setDestination(identity.getDestination());
        eventParser.setSlaveId(slaveId);
        eventParser.setDetectingEnable(false);
        eventParser.setAlarmHandler(alarmHandler);
        AviaterRegexFilter eventFilter = new AviaterRegexFilter(topic == null ? "" : String.join(",", topic));
        eventParser.setEventFilter(eventFilter);
        eventParser.setIsGTIDMode(enableGTID);
        eventParser.setHaController(new HeartBeatHAController());

        AviaterRegexFilter eventBlackFilter = new AviaterRegexFilter("", false);
        eventParser.setEventBlackFilter(eventBlackFilter);

        FileMixedMetaManager metaManager = this.metaManager = new MetaDataFileMixedMetaManager(identity, startupServer, metaPrefix, redisConnectionFactoryBeanName, selectAck2);
        metaManager.setDataDirByFile(dataDir);
        metaManager.setPeriod(1000L);

        FailbackLogPositionManager logPositionManager = new FailbackLogPositionManager(
                new MemoryLogPositionManager(),
                new MetaLogPositionManager(metaManager)
        );
        eventParser.setLogPositionManager(logPositionManager);
        AuthenticationInfo masterInfo = new AuthenticationInfo();
        masterInfo.setAddress(address);
        masterInfo.setUsername(username);
        masterInfo.setPassword(password);
        masterInfo.setEnableDruid(false);
        masterInfo.setDefaultDatabaseName(defaultDatabaseName);
        eventParser.setMasterInfo(masterInfo);

        AuthenticationInfo standbyInfo = new AuthenticationInfo();
        standbyInfo.setAddress(null);
        standbyInfo.setUsername(username);
        standbyInfo.setPassword(password);
        standbyInfo.setEnableDruid(false);
        standbyInfo.setDefaultDatabaseName(defaultDatabaseName);
        eventParser.setStandbyInfo(standbyInfo);

        EntryPosition position = new EntryPosition();
        position.setJournalName("");
        eventParser.setMasterPosition(position);


        EntryPosition standbyPosition = new EntryPosition();
        standbyPosition.setJournalName("");
        eventParser.setStandbyPosition(standbyPosition);

        // ROW,STATEMENT,MIXED
        eventParser.setSupportBinlogFormats("ROW,STATEMENT,MIXED");
        eventParser.setSupportBinlogImages("FULL,MINIMAL,NOBLOB");
        eventParser.setEnableTsdb(false);
        MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        eventStore.setBatchMode(BatchMode.MEMSIZE);
        eventStore.setBufferSize(eventStoreMemoryEnum.getBufferSizeKB());
        eventStore.setBufferMemUnit(1024); // memsize的单位，默认为1kb大小

        EntryEventSink eventSink = new EntryEventSink();
        eventSink.setEventStore(eventStore);
        eventParser.setEventSink(eventSink);

        instance.setEventStore(eventStore);
        instance.setEventSink(eventSink);

        instance.setMetaManager(metaManager);
        instance.setAlarmHandler(alarmHandler);
        instance.setMqConfig(new CanalMQConfig());
        instance.setEventParser(eventParser);
        return instance;
    }

    public void waitMaxDumpThread() throws InterruptedException {
        while (true) {
            if (setDiscard) {
                setDiscard = false;
            }
            List<Map<String, Object>> list;
            try {
                list = selectShowProcesslist(dataSource);
                if (list.isEmpty()) {
                    Thread.sleep(1000);
                    list = selectShowProcesslist(dataSource);
                }
            } catch (SQLException e) {
                continue;
            }
            List<Map<String, Object>> dumpThreadList = list.stream()
                    .filter(e -> Objects.equals(username, e.get("User"))
                            && Objects.equals(Objects.toString(defaultDatabaseName, ""), Objects.toString(e.get("db"), ""))
                            && String.valueOf(e.get("Command")).startsWith("Binlog Dump"))
                    .collect(Collectors.toList());
            if (dumpThreadList.size() < maxDumpThread) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void connect() {
        try {
            waitMaxDumpThread();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!server.isStart()) {
            server.start();
        }
        this.connect = true;
    }

    @Override
    public void subscribe(String[] topic) {
        try {
            waitMaxDumpThread();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (connect) {
            CanalInstanceWithSpring subscribe = this.subscribe = newInstance(topic);
            canalInstanceMap.put(identity.getDestination(), subscribe);

            server.start(identity.getDestination());
            server.subscribe(identity);

            metaManager.setCursor(getCurrentCursor(), false);
        }
    }

    private Position getCurrentCursor() {
        return subscribe.getMetaManager().getCursor(identity);
    }

    @Override
    public void rollback() {
        if (connect) {
            server.rollback(identity);
        }
    }

    @Override
    public void ack() {
        if (connect && lastMessage != null) {
            server.ack(identity, lastMessage.getId());
        }
    }

    @Override
    public Ack2 getAck2() {
        Position currentCursor = getCurrentCursor();
        return new PositionAck2(currentCursor, metaManager);
    }

    @Override
    public List<Dml> getListWithoutAck(Duration timeout) {
        if (setDiscard) {
            this.discardResult = discard();
            this.setDiscard = false;
        }
        Message message = server.getWithoutAck(identity, pullSize, timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (message.getId() == -1) {
            return Collections.emptyList();
        }
        lastMessage = message;
        List<Dml> list = convert(message, destination);
        if (list.isEmpty()) {
            ack();
            return Collections.emptyList();
        } else {
            return list;
        }
    }

    @Override
    public synchronized void disconnect() {
        if (connect) {
            if (server.isStart(identity.getDestination())) {
                server.unsubscribe(identity);
            }
            server.stop(identity.getDestination());
            server.stop();
            this.connect = false;
        }
    }

    @Override
    public void setPullSize(Integer pullSize) {
        this.pullSize = pullSize;
    }

    private Map<String, Object> discard() {
        Long minId = lastMessage == null ? null : lastMessage.getId();
        Long maxId = null;
        while (true) {
            Message message = server.getWithoutAck(identity, 500, null, TimeUnit.MILLISECONDS);
            if (message.getId() == -1) {
                break;
            }
            this.lastMessage = message;
            maxId = message.getId();
            ack();
            getAck2().ack();
        }
        metaManager.setCursor(null, true);
        File file = new File(new File(dataDir, identity.getDestination()), "meta.dat");
        boolean deletedDir = deleteDir(file);
        Map<String, Object> result = new HashMap<>();
        result.put("minId", minId);
        result.put("maxId", maxId);
        return result;
    }

    @Override
    public Map<String, Object> setDiscard(boolean discard) throws InterruptedException {
        this.setDiscard = discard;
        if (connect && discard) {
            while (this.setDiscard) {
                Thread.sleep(100);
            }
            return discardResult;
        }
        return null;
    }

    private synchronized void rebuild(String clientIdentityName, String message) {
        if (rebuildConsumer != null) {
            File file = new File(new File(dataDir, clientIdentityName), "meta.dat");
            boolean deletedDir = deleteDir(file);
            log.info("=============> rebuild destination: {} <============= file='{}', delete={}, lastMessage={}", clientIdentityName, file, deletedDir, lastMessage);
            if (deletedDir) {
                lastMessage = null;
            }
            rebuildConsumer.accept(MysqlBinlogCanalConnector.this);
        }
    }

    private static class PositionAck2 implements Ack2 {
        private final Position currentCursor;
        private final MetaDataFileMixedMetaManager metaManager;

        private PositionAck2(Position currentCursor, MetaDataFileMixedMetaManager metaManager) {
            this.currentCursor = currentCursor;
            this.metaManager = metaManager;
        }

        @Override
        public void ack() {
            metaManager.setCursor(currentCursor, true);
        }

        @Override
        public String toString() {
            return currentCursor.toString();
        }
    }

    public static class MetaDataFileMixedMetaManager extends FileMixedMetaManager {
        private final ClientIdentity identity;
        private final MetaDataRepository metaDataRepository;
        private final AtomicBoolean cursorChange = new AtomicBoolean();
        private final boolean selectAck2;
        private ScheduledExecutorService scheduled;
        private volatile Object cursor;
        private long period = 1000L;

        public MetaDataFileMixedMetaManager(ClientIdentity identity, StartupServer startupServer, String prefix, String redisConnectionFactoryBeanName, boolean selectAck2) {
            this.identity = identity;
            this.selectAck2 = selectAck2;
            this.metaDataRepository = MetaDataRepository.newInstance(
                    prefix + ":" + identity.getDestination(), redisConnectionFactoryBeanName, startupServer.getBeanFactory());
        }

        @Override
        public void setPeriod(long period) {
            super.setPeriod(period);
            this.period = period;
        }

        public void setCursor(Object cursor, boolean change) {
            this.cursor = cursor;
            if (change) {
                this.cursorChange.set(true);
            }
        }

        @Override
        public void start() {
            super.start();
            if (metaDataRepository != null) {
                scheduled = Util.newScheduled(1, metaDataRepository::name, e -> log.warn("Scheduled error {}", e.toString(), e));
                scheduled.scheduleAtFixedRate(() -> {
                    if (cursorChange.compareAndSet(true, false)) {
                        Object cursor = this.cursor;
                        metaDataRepository.setCursor(cursor);
                    }
                }, period, period, TimeUnit.MILLISECONDS);

                if (selectAck2) {
                    Position cursor = metaDataRepository.getCursor();
                    if (cursor != null) {
                        updateCursor(identity, cursor);
                    }
                }
            }
        }


        @Override
        public void stop() {
            super.stop();
            if (metaDataRepository != null) {
                metaDataRepository.close();
            }
            if (scheduled != null) {
                scheduled.shutdown();
            }
        }
    }

    class AlarmHandler extends LogAlarmHandler {
        @Override
        public void sendAlarm(String clientIdentityName, String msg) {
            super.sendAlarm(clientIdentityName, msg);
            if (msg != null) {
                if (msg.contains("Could not find first log file name in binary log index file")
                        || msg.contains("Timeout occurred")
                ) {
                    rebuild(clientIdentityName, msg);
                }
            }
        }
    }
}
