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
 * 根据自增ID的全量灌数据，可以继承这个Controller
 * <p>
 * curl "http://localhost:8080/es7x/job/syncById?id=1,2"
 * curl "http://localhost:8080/es7x/job/syncAll"
 * curl "http://localhost:8080/es7x/job/stop"
 */
public abstract class AbstractEs7xETLIntController {
    private static final Logger log = LoggerFactory.getLogger(AbstractEs7xETLIntController.class);

    private final ExecutorService executorService = Util.newFixedThreadPool(1000, 5000L,
            getClass().getSimpleName(), true);
    @Autowired(required = false)
    protected StartupServer startupServer;
    @Autowired(required = false)
    protected AbstractMessageService messageService;
    private boolean stop = false;

    private static Integer getDmlListMaxId(List<Dml> list) {
        int maxId = Integer.MIN_VALUE;
        for (Dml dml : list) {
            List<String> pkNames = dml.getPkNames();
            if (pkNames.size() != 1) {
                throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
            }
            String pkName = pkNames.iterator().next();
            for (Map<String, Object> row : dml.getData()) {
                Object o = row.get(pkName);
                if (o == null) {
                    continue;
                }
                int id;
                if (o instanceof Integer) {
                    id = (Integer) o;
                } else {
                    id = Integer.parseInt(o.toString());
                }
                if (id > maxId) {
                    maxId = id;
                }
            }
        }
        return maxId == Integer.MIN_VALUE ? null : maxId;
    }

    protected abstract Integer selectMaxId(JdbcTemplate jdbcTemplate);

    protected abstract ES7xAdapter getES7xAdapter();

    protected abstract List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Integer minId, int limit);

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Integer minId, int limit, String tableName, String idColumnName) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Arrays.asList(row), Arrays.asList(idColumnName), tableName, catalog));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, Integer minId, int limit, String tableName, String idColumnName) {
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
    public List<SyncRunnable> syncAll(
            @RequestParam(required = false, defaultValue = "50") int threads,
            @RequestParam(required = false, defaultValue = "0") int offsetStart,
            Integer offsetEnd,
            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
            @RequestParam(required = false, defaultValue = "defaultDS") String ds,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "false") boolean discard) {
        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(ds);
        String catalog = CanalConfig.DatasourceConfig.getCatalog(ds);

        String clientIdentity = getES7xAdapter().getClientIdentity();
        if (discard) {
            new Thread(() -> {
                try {
                    discard(clientIdentity);
                } catch (InterruptedException e) {
                    log.info("discard {}", e, e);
                }
            }).start();
        }

        setSuspendEs7x(true, clientIdentity);
        this.stop = false;

        Integer maxId = offsetEnd == null || offsetEnd == Integer.MAX_VALUE ?
                selectMaxId(jdbcTemplate) : offsetEnd;

        List<SyncRunnable> runnableList = new ArrayList<>();
        Date timestamp = new Timestamp(System.currentTimeMillis());
        AtomicInteger done = new AtomicInteger(0);
        AtomicInteger dmlSize = new AtomicInteger(0);
        for (int i = 0; i < threads; i++) {
            runnableList.add(new SyncRunnable(getClass().getSimpleName(), messageService, i, offsetStart, maxId, threads) {
                @Override
                public int run0(int offset) {
                    if (stop) {
                        return Integer.MAX_VALUE;
                    }
                    List<Dml> list = syncAll(jdbcTemplate, catalog, offset, offsetAdd, append);
                    if (log.isInfoEnabled()) {
                        log.info("syncAll minOffset {}", SyncRunnable.minOffset(runnableList));
                    }
                    dmlSize.addAndGet(list.size());
                    Integer dmlListMaxId = getDmlListMaxId(list);
                    return dmlListMaxId == null ? Integer.MAX_VALUE : dmlListMaxId;
                }

                @Override
                public void done() {
                    if (done.incrementAndGet() == threads) {
                        if (log.isInfoEnabled()) {
                            log.info("syncAll done {}", this);
                        }
                        setSuspendEs7x(false, clientIdentity);
                        sendDone(runnableList, timestamp, dmlSize.intValue());
                    }
                }
            });
        }
        for (SyncRunnable runnable : runnableList) {
            executorService.execute(runnable);
        }
        return runnableList;
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam Integer[] id,
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

    protected List<Dml> syncAll(JdbcTemplate jdbcTemplate, String catalog, Integer minId, int offsetAdd, boolean append) {
        List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, minId, offsetAdd);
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
            Integer dmlListMaxId = getDmlListMaxId(dmlList);
            Set<Map<String, ESSyncConfig>> configMapList = Collections.newSetFromMap(new IdentityHashMap<>());
            for (String tableName : tableNameSet) {
                configMapList.addAll(esAdapter.getEsSyncConfig(catalog, tableName));
            }
            for (Map<String, ESSyncConfig> configMap : configMapList) {
                for (ESSyncConfig config : configMap.values()) {
                    esAdapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, minId, dmlListMaxId, offsetAdd);
                }
            }
        }
        esAdapter.sync(dmlList, false, true);
        return dmlList;
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<Integer> id) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        ES7xAdapter esAdapter = getES7xAdapter();
        int count = 0;
        for (Integer i : id) {
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1);
            for (Dml dml : dmlList) {
                dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
            }
            esAdapter.sync(dmlList, false, true);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(List<SyncRunnable> runnableList, Date startTime, int dmlSize) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n 明细 = " + runnableList;
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
        private final String name;
        protected int cOffset;
        private boolean done;

        public SyncRunnable(String name, AbstractMessageService messageService, int threadIndex, int offsetStart, int maxId, int threads) {
            this.name = name;
            this.messageService = messageService;
            this.threadIndex = threadIndex;
            this.maxId = maxId;
            this.threads = threads;
            this.offsetStart = offsetStart;
            int allocation = ((maxId + 1 - offsetStart) / threads);
            this.offset = offsetStart + (threadIndex * allocation);
            this.endOffset = threadIndex + 1 == threads ? offset + allocation * 3 : offset + allocation;
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
                    + ",\n\n 对象 = " + name
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
                    if (cOffset == Integer.MAX_VALUE) {
                        break;
                    }
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