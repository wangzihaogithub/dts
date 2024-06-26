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
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.Dml;
import com.github.dts.util.Util;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mysql.cj.jdbc.MysqlDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MysqlBinlogCanalConnector implements CanalConnector {
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
    private CanalInstanceWithSpring subscribe;
    private Integer pullSize;
    private Message lastMessage;
    private volatile Map<String, Object> discardResult;
    private volatile boolean setDiscard = false;
    private Consumer<CanalConnector> rebuildConsumer;
    private boolean connect;

    public MysqlBinlogCanalConnector(CanalConfig canalConfig, CanalConfig.CanalAdapter config) throws URISyntaxException {
        Properties properties = config.getProperties();

        String destination = config.getDestination();
        this.identity = new ClientIdentity(destination, (short) 1001);
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

        this.enableGTID = "true".equalsIgnoreCase(properties.getProperty("enableGTID", "false"));
        this.slaveId = Integer.parseInt(properties.getProperty("slaveId", String.valueOf(generateUniqueServerId(destination, Util.getIPAddress()))));
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

    public static List<Dml> convert(Message message, String destination) {
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
                new RdsBinlogEventParserProxy();
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

        int period = 1000;
        FileMixedMetaManager metaManager = new FileMixedMetaManager();
        metaManager.setDataDirByFile(dataDir);
        metaManager.setPeriod(period);

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
        try {
            while (true) {
                if (setDiscard) {
                    setDiscard = false;
                }
                List<Map<String, Object>> list = selectShowProcesslist(dataSource);
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
        } catch (SQLException e) {
            Util.sneakyThrows(e);
        }
    }

    @Override
    public void connect() {
        try {
            waitMaxDumpThread();
        } catch (InterruptedException e) {
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
            return;
        }

        if (connect) {
            CanalInstanceWithSpring subscribe = this.subscribe = newInstance(topic);
            canalInstanceMap.put(identity.getDestination(), subscribe);

            server.start(identity.getDestination());
            server.subscribe(identity);
        }
    }

    @Override
    public void rollback() {
        if (connect) {
            server.rollback(identity);
        }
    }

    @Override
    public void ack() {
        if (lastMessage != null) {
            server.ack(identity, lastMessage.getId());
        }
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
        List<Dml> list = convert(message, identity.getDestination());
        if (list.isEmpty()) {
            ack();
            return Collections.emptyList();
        } else {
            return list;
        }
    }

    @Override
    public void disconnect() {
        if (connect) {
            server.unsubscribe(identity);
            server.stop(identity.getDestination());
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
        }
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

    class AlarmHandler extends LogAlarmHandler {
        @Override
        public void sendAlarm(String destination, String msg) {
            super.sendAlarm(destination, msg);
            if (msg != null && msg.contains("Could not find first log file name in binary log index file")) {
                if (rebuildConsumer != null) {
                    File file = new File(new File(dataDir, destination), "meta.dat");
                    if (deleteDir(file)) {
                        lastMessage = null;
                    }
                    rebuildConsumer.accept(MysqlBinlogCanalConnector.this);
                }
            }
        }
    }
}
