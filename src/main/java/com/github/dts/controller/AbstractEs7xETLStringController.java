package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 根据非自增ID的全量灌数据，可以继承这个Controller
 * <p>
 * id	group_name	name	home_url	source_enum	data_type	credit_code	register_status_name	nature_enum	manual_flag	unique_key	status	create_user	update_user	create_time	update_time	root_name
 * 2024071120001536320040986	陕西煤业化工集团	海南德璟置业投资有限责任公司		tyc	0	91460200056399292A	存续	guo_qi	0	662f11edbe7fa41ef500f138	1			2024-07-11 20:00:15	2024-07-11 20:00:15	陕西煤业化工集团
 * 2024071120001540020040987	陕西煤业化工集团	西安重装渭南橡胶制品有限公司		tyc	0	916105005735206015	存续	guo_qi	0	662e1a9ebe7fa41ef5ea41fc	1			2024-07-11 20:00:15	2024-07-11 20:00:15	陕西煤业化工集团
 * 2024071120001546920040988	仁怀市建工集团	仁怀城投中资智慧城市运营有限公司		tyc	0	91520382MAC57C207X	存续	guo_qi	0	662f11edbe7fa41ef500f12e	1			2024-07-11 20:00:15	2024-07-11 20:00:15	仁怀市建工集团
 * 2024071120001563920040989	苏州城市建设投资发展集团	苏州物资控股（集团）有限责任公司		tyc	0	913205001377629700	存续	guo_qi	0	664c7375a5bca3e4f734c87d	1			2024-07-11 20:00:16	2024-07-11 20:00:16	苏州城市建设投资发展集团
 * <p>
 * curl "http://localhost:8080/es7x/corp/syncById?id=2024071120255559720056013,2024071118325561520000001"
 * curl "http://localhost:8080/es7x/corp/syncAll"
 * curl "http://localhost:8080/es7x/corp/stop"
 */
public abstract class AbstractEs7xETLStringController {
    private static final Logger log = LoggerFactory.getLogger(AbstractEs7xETLStringController.class);

    private final ExecutorService executorService = Util.newFixedThreadPool(1000, 5000L,
            getClass().getSimpleName(), true);
    @Autowired(required = false)
    protected StartupServer startupServer;
    @Autowired(required = false)
    protected AbstractMessageService messageService;
    private boolean stop = false;

    private static String getDmlListMaxId(List<Dml> list) {
        if (list.isEmpty()) {
            return null;
        }
        Dml dml = list.get(list.size() - 1);
        List<String> pkNames = dml.getPkNames();
        if (pkNames.size() != 1) {
            throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
        }
        String pkName = pkNames.iterator().next();
        Map<String, Object> last = dml.getData().get(dml.getData().size() - 1);
        Object o = last.get(pkName);
        return o == null || "".equals(o) ? null : o.toString();
    }

    protected abstract ES7xAdapter getES7xAdapter();

    protected abstract List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit);

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit, String tableName, String idColumnName) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Arrays.asList(row), Arrays.asList(idColumnName), tableName, catalog));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, String minId, int limit, String tableName, String idColumnName) {
        String sql;
        if (limit == 1) {
            sql = "select * from " + tableName + " where " + idColumnName + " = ? limit ?";
        } else {
            sql = "select * from " + tableName + " where " + idColumnName + " > ? limit ?";
        }
        return jdbcTemplate.queryForList(sql, minId, limit);
    }

    protected ES7xAdapter getES7xAdapter(String name) {
        return startupServer.getAdapter(name, ES7xAdapter.class);
    }

    @RequestMapping("/syncAll")
    public String syncAll(
            @RequestParam(required = false, defaultValue = "0") String offsetStart,
            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
            @RequestParam(required = false, defaultValue = "defaultDS") String ds,
            @RequestParam(required = false, defaultValue = "true") boolean append) {
        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(ds);
        String catalog = CanalConfig.DatasourceConfig.getCatalog(ds);
        executorService.execute(() -> {
            String minId = offsetStart;
            Date timestamp = new Timestamp(System.currentTimeMillis());
            AtomicInteger dmlSize = new AtomicInteger(0);
            try {
                List<Dml> list;
                do {
                    list = syncAll(jdbcTemplate, catalog, minId, offsetAdd, append);
                    if (log.isInfoEnabled()) {
                        log.info("syncAll minOffset {}", minId);
                    }
                    dmlSize.addAndGet(list.size());
                    minId = getDmlListMaxId(list);
                    if (stop) {
                        break;
                    }
                } while (minId != null);
                sendDone(timestamp, dmlSize.intValue());
            } catch (Exception e) {
                sendError(e, minId, timestamp, dmlSize.get());
                throw e;
            }
        });
        return catalog;
    }

    private void sendError(Throwable throwable, String minId, Date timestamp, int dmlSize) {
        String title = "ES搜索全量刷数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n minOffset = " + minId
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 异常 = " + throwable
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam String[] id,
                           @RequestParam(required = false, defaultValue = "defaultDS") String ds) {
        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(ds);
        String catalog = CanalConfig.DatasourceConfig.getCatalog(ds);
        stop = false;
        int count = 0;
        try {
            count = id.length;
            syncById(jdbcTemplate, catalog, Arrays.asList(id));
        } finally {
            log.info("all sync end.  total = {} ", count);
        }
        return 1;
    }

    @RequestMapping("/stop")
    public boolean stop() {
        stop = true;
        return stop;
    }

    private List<Map> discard(String clientIdentity) throws InterruptedException {
        List<Map> list = new ArrayList<>();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(clientIdentity)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    private void setSuspendEs7x(boolean suspend, String clientIdentity) {
        List<StartupServer.ThreadRef> canalThread = startupServer.getCanalThread(clientIdentity);
        for (StartupServer.ThreadRef thread : canalThread) {
            if (suspend) {
                thread.stopThread();
            } else {
                thread.startThread();
            }
        }
    }

    protected Integer selectMaxId(JdbcTemplate jdbcTemplate, String idFiled, String tableName) {
        return jdbcTemplate.queryForObject("select max(" + idFiled + ") from " + tableName, Integer.class);
    }

    protected List<Dml> syncAll(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit, boolean append) {
        List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, minId, limit);
        if (dmlList.isEmpty()) {
            return dmlList;
        }
        ES7xAdapter esAdapter = getES7xAdapter();
        for (Dml dml : dmlList) {
            dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
        }
        if (!append) {
            Set<String> tableNameSet = new LinkedHashSet<>(2);
            for (Dml dml : dmlList) {
                tableNameSet.add(dml.getTable());
            }
            String dmlListMaxId = getDmlListMaxId(dmlList);
            Set<Map<String, ESSyncConfig>> configMapList = Collections.newSetFromMap(new IdentityHashMap<>());
            for (String tableName : tableNameSet) {
                configMapList.addAll(esAdapter.getEsSyncConfig(catalog, tableName));
            }
            for (Map<String, ESSyncConfig> configMap : configMapList) {
                for (ESSyncConfig config : configMap.values()) {
                    ESBulkRequest.ESBulkResponse esBulkResponse = esAdapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, minId, dmlListMaxId, limit);
                    esBulkResponse.isEmpty();
                }
            }
        }
        esAdapter.sync(dmlList, false, true);
        return dmlList;
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<String> id) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        ES7xAdapter esAdapter = getES7xAdapter();
        int count = 0;
        for (String i : id) {
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1);
            for (Dml dml : dmlList) {
                dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
            }
            esAdapter.sync(dmlList, false, true);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(Date startTime, int dmlSize) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 对象 = " + getClass().getSimpleName();
        messageService.send(title, content);
    }

    public static abstract class SyncRunnable implements Runnable {
        private static final List<SyncRunnable> RUNNABLE_LIST = Collections.synchronizedList(new ArrayList<>());
        protected final int threadIndex;
        private final int maxId;
        private final int threads;
        private final int offset;
        private final int endOffset;
        private final int offsetStart;
        private final AbstractMessageService messageService;
        protected int cOffset;
        private boolean done;

        public SyncRunnable(AbstractMessageService messageService, int threadIndex, int offsetStart, int maxId, int threads) {
            this.messageService = messageService;
            this.threadIndex = threadIndex;
            this.maxId = maxId;
            this.threads = threads;
            this.offsetStart = offsetStart;
            int allocation = ((maxId + 1 - offsetStart) / threads);
            this.offset = offsetStart + (threadIndex * allocation);
            this.endOffset = threadIndex + 1 == threads ? offset + allocation * 2 : offset + allocation;
            this.cOffset = offset;
            RUNNABLE_LIST.add(this);
        }

        public static Integer minOffset(List<SyncRunnable> list) {
            Integer min = null;
            for (SyncRunnable runnable : list) {
                if (!runnable.done) {
                    if (min == null) {
                        min = runnable.cOffset;
                    } else {
                        min = Math.min(min, runnable.cOffset);
                    }
                }
            }
            return min;
        }

        private void sendError(Throwable throwable) {
            String title = "ES搜索全量刷数据-异常";
            StringWriter writer = new StringWriter();
            throwable.printStackTrace(new PrintWriter(writer));

            Integer minOffset = minOffset(RUNNABLE_LIST);

            String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                    + " \n\n   ---  "
                    + ",\n\n threadIndex = " + threadIndex
                    + ",\n\n minOffset = " + minOffset
                    + ",\n\n currentOffset = " + cOffset
                    + ",\n\n maxId = " + maxId
                    + ",\n\n offset = " + offset
                    + ",\n\n offsetStart = " + offsetStart
                    + ",\n\n endOffset = " + endOffset
                    + ",\n\n threads = " + threads
                    + ",\n\n 异常 = " + throwable
                    + ",\n\n 明细 = " + writer;
            messageService.send(title, content);
        }

        public boolean isDone() {
            return done;
        }

        @Override
        public void run() {
            int i = 0;
            try {
                for (cOffset = offset, i = 0; cOffset < endOffset; i++) {
                    int cOffsetbefore = cOffset;
                    cOffset = run0(cOffset);
                    log.info("all sync threadIndex {}/{}, offset = {}-{}, i ={}, maxId = {}",
                            threadIndex, threads, cOffsetbefore, cOffset, i, maxId);
                    if (cOffsetbefore == cOffset && cOffsetbefore == maxId) {
                        break;
                    }
                }
            } catch (Exception e) {
                sendError(e);
                throw e;
            } finally {
                done = true;
                log.info("all sync done threadIndex {}/{}, offset = {}, i ={}, maxId = {}, info ={} ",
                        threadIndex, threads, cOffset, i, maxId, this);
                done();
            }
        }

        public void done() {

        }

        public abstract int run0(int offset);

        public int getThreadIndex() {
            return threadIndex;
        }

        public int getMaxId() {
            return maxId;
        }

        public int getThreads() {
            return threads;
        }

        public int getOffset() {
            return offset;
        }

        public int getEndOffset() {
            return endOffset;
        }

        public int getOffsetStart() {
            return offsetStart;
        }

        @Override
        public String toString() {
            return "SyncRunnable{" +
                    "threadIndex=" + threadIndex +
                    ", maxId=" + maxId +
                    ", threads=" + threads +
                    ", cOffset=" + cOffset +
                    ", offset=" + offset +
                    ", endOffset=" + endOffset +
                    ", offsetStart=" + offsetStart +
                    '}';
        }
    }
}