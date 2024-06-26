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

public abstract class AbstractEs7xETLController {
    private static final Logger log = LoggerFactory.getLogger(AbstractEs7xETLController.class);

    private final ExecutorService executorService = Util.newFixedThreadPool(1000, 5000L,
            "controller", true);
    @Autowired(required = false)
    protected StartupServer startupServer;
    @Autowired(required = false)
    protected AbstractMessageService messageService;
    private boolean stop = false;

    protected abstract Integer selectMaxId(JdbcTemplate jdbcTemplate);
//    {
//        return selectMaxId(jdbcTemplate, "id", "biz_job");
//    }

    protected abstract ES7xAdapter getES7xAdapter();
//    {
//         return startupServer.getAdapter(name, ES7xAdapter.class);
//    }

    protected abstract List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Integer minId, Integer maxId);
//    {
//        jdbcTemplate.queryForList(
//                "select id from " + J_TABLE_NAME + " where id between ? and ?", Integer.class,
//                offset, endOffset)
//        Dml.convertInsert()
//    }

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
            @RequestParam(required = false, defaultValue = "true") boolean append) {
        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(ds);
        String catalog = CanalConfig.DatasourceConfig.getCatalog(ds);

        String destination = getES7xAdapter().getDestination();
        new Thread(() -> {
            try {
                discard(destination);
            } catch (InterruptedException e) {
                log.info("discard {}", e, e);
            }
        }).start();

        setSuspendEs7x(true, destination);
        this.stop = false;

        Integer maxId = offsetEnd == null || offsetEnd == Integer.MAX_VALUE ?
                selectMaxId(jdbcTemplate) : offsetEnd;

        List<SyncRunnable> runnableList = new ArrayList<>();
        Date timestamp = new Timestamp(System.currentTimeMillis());
        AtomicInteger done = new AtomicInteger(0);
        AtomicInteger dmlSize = new AtomicInteger(0);
        for (int i = 0; i < threads; i++) {
            runnableList.add(new SyncRunnable(messageService, i, offsetStart, maxId, threads) {
                @Override
                public int run0(int offset) {
                    if (stop) {
                        return Integer.MAX_VALUE;
                    }
                    int endOffset = offset + offsetAdd;
                    if (offsetEnd != null) {
                        endOffset = Math.min(offsetEnd, endOffset);
                    }
                    int sync = syncAll(jdbcTemplate, catalog, offset, endOffset, append);
                    if (log.isInfoEnabled()) {
                        log.info("syncAll minOffset {}", SyncRunnable.minOffset(runnableList));
                    }
                    dmlSize.addAndGet(sync);
                    return endOffset;
                }

                @Override
                public void done() {
                    if (done.incrementAndGet() == threads) {
                        if (log.isInfoEnabled()) {
                            log.info("syncAll done {}", this);
                        }
                        setSuspendEs7x(false, destination);
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

    @RequestMapping("/suspend")
    public boolean suspend(boolean suspend) {
        setSuspendEs7x(suspend, getES7xAdapter().getDestination());
        return suspend;
    }

    @RequestMapping("/discard")
    public List<Map> discard() throws InterruptedException {
        return discard(getES7xAdapter().getDestination());
    }

    @RequestMapping("/stop")
    public boolean stop() {
        stop = true;
        return stop;
    }

    private List<Map> discard(String destination) throws InterruptedException {
        List<Map> list = new ArrayList<>();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(destination)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    private void setSuspendEs7x(boolean suspend, String destination) {
        List<StartupServer.ThreadRef> canalThread = startupServer.getCanalThread(destination);
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

    protected int syncAll(JdbcTemplate jdbcTemplate, String catalog, Integer minId, Integer maxId, boolean append) {
        List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, minId, maxId);
        ES7xAdapter esAdapter = getES7xAdapter();
        for (Dml dml : dmlList) {
            dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
        }
        if (!append) {
            Set<String> tableNameSet = new LinkedHashSet<>(2);
            for (Dml dml : dmlList) {
                tableNameSet.add(dml.getTable());
            }
            List<Map<String, ESSyncConfig>> configMapList = new ArrayList<>();
            for (String tableName : tableNameSet) {
                Map<String, ESSyncConfig> configMap = esAdapter.getEsSyncConfig(catalog, tableName);
                if (configMap != null) {
                    configMapList.add(configMap);
                }
            }
            for (Map<String, ESSyncConfig> configMap : configMapList) {
                for (ESSyncConfig config : configMap.values()) {
                    ESBulkRequest.ESBulkResponse esBulkResponse = esAdapter.getEsTemplate().deleteByIdRange(config.getEsMapping(), minId, maxId);
                    esBulkResponse.isEmpty();
                }
            }
        }
        esAdapter.sync(dmlList, false);
        return dmlList.size();
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<Integer> id) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        ES7xAdapter esAdapter = getES7xAdapter();
        int count = 0;
        for (Integer i : id) {
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, i);
            for (Dml dml : dmlList) {
                dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
            }
            esAdapter.sync(dmlList, false);
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