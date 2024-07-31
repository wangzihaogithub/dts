package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.util.*;
import org.apache.commons.lang3.StringUtils;
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
import java.util.stream.Collectors;

/**
 * 根据日期全量灌数据，可以继承这个Controller
 * <pre>
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncAll?fieldName=create_time&offsetStart=2022-03-01&offsetEnd=2024-10-01">http://localhost:8080/es7x/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/stop">http://localhost:8080/es7x/myxxx/stop</a>"
 * </pre>
 */
public abstract class AbstractEs7xETLDateController {
    private static final Logger log = LoggerFactory.getLogger(AbstractEs7xETLDateController.class);

    private final ExecutorService executorService = Util.newFixedThreadPool(1000, 5000L,
            getClass().getSimpleName(), true);
    @Autowired(required = false)
    protected StartupServer startupServer;
    @Autowired(required = false)
    protected AbstractMessageService messageService;
    private boolean stop = false;

    protected abstract Date selectMaxDate(JdbcTemplate jdbcTemplate);

    protected abstract ES7xAdapter getES7xAdapter();

    protected abstract List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Timestamp minId, Timestamp maxId);

    protected ES7xAdapter getES7xAdapter(String name) {
        return startupServer.getAdapter(name, ES7xAdapter.class);
    }

    @RequestMapping("/syncAll")
    public List<SyncRunnable> syncAll(
            @RequestParam(required = false, defaultValue = "50") int threads,
            @RequestParam(required = false, defaultValue = "0") String offsetStart,
            String offsetEnd,
            @RequestParam String fieldName,
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "600000") long offsetAdd,
            @RequestParam(required = false, defaultValue = "defaultDS") String ds,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "false") boolean discard,
            @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }
        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(ds);
        String catalog = CanalConfig.DatasourceConfig.getCatalog(ds);

        Date offsetStartParse = DateUtil.parseDate(offsetStart);
        Date offsetEndParse = DateUtil.parseDate(offsetEnd);

        long offsetStartDate = offsetStartParse.getTime();
        Long offsetEndDate = offsetEndParse == null ? null : offsetEndParse.getTime();

        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        String clientIdentity = getES7xAdapter().getClientIdentity();
        if (discard) {
            new Thread(() -> {
                try {
                    discard(clientIdentity);
                } catch (InterruptedException e) {
                    log.info("discard {}", e.toString(), e);
                }
            }).start();
        }

        List<SyncRunnable> runnableList = new ArrayList<>();
        setSuspendEs7x(true, clientIdentity);
        this.stop = false;
        for (ES7xAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                long maxId = offsetEndDate == null ?
                        selectMaxDate(jdbcTemplate).getTime() : offsetEndDate;

                Date timestamp = new Timestamp(System.currentTimeMillis());
                AtomicInteger done = new AtomicInteger(0);
                AtomicInteger dmlSize = new AtomicInteger(0);
                for (int i = 0; i < threads; i++) {
                    runnableList.add(new SyncRunnable(getClass().getSimpleName(), messageService, i, offsetStartDate, maxId, threads) {
                        @Override
                        public long run0(long offset) {
                            if (stop) {
                                return Long.MAX_VALUE;
                            }
                            long endOffset = offset + offsetAdd;
                            if (offsetEndDate != null) {
                                endOffset = Math.min(offsetEndDate, endOffset);
                            }
                            int sync = syncAll(jdbcTemplate, catalog, fieldName, offset, endOffset, append, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet, adapter, config);
                            dmlSize.addAndGet(sync);
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.intValue(), SyncRunnable.minOffset(runnableList));
                            }
                            return endOffset;
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
            }
        }
        return runnableList;
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

    protected Date selectMaxDate(JdbcTemplate jdbcTemplate, String idFiled, String tableName) {
        return jdbcTemplate.queryForObject("select max(" + idFiled + ") from " + tableName, Date.class);
    }

    protected int syncAll(JdbcTemplate jdbcTemplate, String catalog, String fieldName,
                          long minId, long maxId, boolean append, boolean onlyCurrentIndex, int joinUpdateSize, Collection<String> onlyFieldName,
                          ES7xAdapter adapter, ESSyncConfig config) {
        Timestamp minIdDate = new Timestamp(minId);
        Timestamp maxIdDate = new Timestamp(maxId);

        List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, minIdDate, maxIdDate);
        if (dmlList.isEmpty()) {
            return 0;
        }
        ES7xAdapter esAdapter = getES7xAdapter();
        for (Dml dml : dmlList) {
            dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
        }
        if (!append) {
            ESBulkRequest.ESBulkResponse esBulkResponse = esAdapter.getEsTemplate().deleteByRange(config.getEsMapping(), fieldName, minIdDate, maxIdDate, null);
        }
        esAdapter.sync(dmlList, false, true, onlyCurrentIndex, joinUpdateSize, onlyFieldName);
        return dmlList.size();
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
        private final long maxId;
        private final int threads;
        private final long offset;
        private final long endOffset;
        private final long offsetStart;
        private final AbstractMessageService messageService;
        private final String name;
        protected long cOffset;
        private boolean done;

        public SyncRunnable(String name, AbstractMessageService messageService, int threadIndex, long offsetStart, long maxId, int threads) {
            this.name = name;
            this.messageService = messageService;
            this.threadIndex = threadIndex;
            this.maxId = maxId;
            this.threads = threads;
            this.offsetStart = offsetStart;
            long allocation = ((maxId + 1 - offsetStart) / threads);
            this.offset = offsetStart + (threadIndex * allocation);
            this.endOffset = threadIndex + 1 == threads ? offset + allocation * 2 : offset + allocation;
            this.cOffset = offset;
            RUNNABLE_LIST.add(this);
        }

        public static Long minOffset(List<SyncRunnable> list) {
            Long min = null;
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
            String title = "ES搜索日期全量刷数据-异常";
            StringWriter writer = new StringWriter();
            throwable.printStackTrace(new PrintWriter(writer));

            Long minOffset = minOffset(RUNNABLE_LIST);

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
            long i = 0;
            try {
                for (cOffset = offset, i = 0; cOffset < endOffset; i++) {
                    long cOffsetbefore = cOffset;
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

        public abstract long run0(long offset);

        public int getThreadIndex() {
            return threadIndex;
        }

        public long getMaxId() {
            return maxId;
        }

        public int getThreads() {
            return threads;
        }

        public long getOffset() {
            return offset;
        }

        public long getEndOffset() {
            return endOffset;
        }

        public long getOffsetStart() {
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