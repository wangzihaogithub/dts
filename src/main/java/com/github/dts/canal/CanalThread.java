package com.github.dts.canal;

import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class CanalThread extends Thread {
    private static final int DEFAULT_BATCH_SIZE = 50;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final CanalConnector connector;
    private final CanalConfig.CanalAdapter config;
    private final AbstractMessageService messageService;
    protected String groupId = null;                                                  // groupId
    protected List<Adapter> adapterList;                                              // 外部适配器
    protected CanalConfig canalConfig;                                               // 配置
    protected volatile ExecutorService executorService;                                       // 组内工作线程池
    protected volatile boolean running = false;                                                 // 是否运行中
    protected Thread thread = null;
    protected UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    private String name;
    private boolean suspend;

    /**
     * 单台client适配器worker的构造方法
     *
     * @param adapterList 外部适配器组
     */
    public CanalThread(CanalConfig canalConfig, CanalConfig.CanalAdapter config,
                       List<Adapter> adapterList, AbstractMessageService messageService, CanalConnector connector,
                       Consumer<CanalThread> rebuild) {
        this.adapterList = adapterList;
        this.canalConfig = canalConfig;
        this.config = config;
        this.messageService = messageService;
        this.name = config.getDestination() + limit(connector.getClass().getSimpleName(), 8);

        this.connector = connector;
        connector.rebuildConsumer(new Consumer<CanalConnector>() {
            @Override
            public void accept(CanalConnector integer) {
                rebuild.accept(CanalThread.this);
            }
        });
        setName(name);
    }

    private static String limit(String str, int limit) {
        if (str != null && str.length() > limit) {
            return str.substring(0, limit);
        } else {
            return str;
        }
    }

    public void setSuspend(boolean suspend) {
        this.suspend = suspend;
    }

    public CanalConnector getConnector() {
        return connector;
    }

    @Override
    public void run() {
        while (!running) { // waiting until running == true
            while (!running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
        int retry = Integer.MAX_VALUE;
        Integer batchSize = config.getBatchSize();
        if (batchSize == null) {
            batchSize = DEFAULT_BATCH_SIZE;
        }
        connector.setPullSize(batchSize);
        Exception exception = null;
        long lastErrorTimestamp = 0;

        while (running) {
            try {
                logger.info("=============> Start to connect destination: {} <=============", this.name);
                connector.connect();
                logger.info("=============> Start to subscribe destination: {} <=============", this.name);
                connector.subscribe(config.getTopics());
                logger.info("=============> Subscribe destination: {} succeed <=============", this.name);
                connector.rollback();
                while (running) {
                    if (!running) {
                        break;
                    }
                    for (int i = 0; i < retry; i++) {
                        if (!running) {
                            break;
                        }
                        List<Dml> message = connector.getListWithoutAck(Duration.ofMillis(config.getPullTimeout())); // 获取指定数量的数据
                        for (Dml dml : message) {
                            dml.setDestination(config.getDestination());
                        }
                        if (suspend) {
                            Thread.sleep(10_000);
                        } else {
                            String batchId = messageID(message);
                            String[] offset = logfileOffset(message, 20);
                            try {
                                int size = message.size();
                                if (message.isEmpty()) {
                                    Thread.sleep(500);
                                } else {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("destination: {} batchId: {} batchSize: {} offset: {}",
                                                this.name,
                                                batchId,
                                                size,
                                                offset);
                                    }
                                    long begin = System.currentTimeMillis();
                                    writeOut(message);
                                    if (logger.isTraceEnabled()) {
                                        logger.debug("destination: {} batchId: {} elapsed time: {} ms",
                                                this.name,
                                                batchId,
                                                System.currentTimeMillis() - begin);
                                    }
                                    connector.ack(); // 提交确认
                                    if (exception != null) {
                                        sendRecover(exception, batchId, offset);
                                        lastErrorTimestamp = 0;
                                        exception = null;
                                    }
                                }
                            } catch (Exception e) {
                                exception = e;
                                logger.error("=============={}======>Error sync not ACK!<====================", e, e);
                                if (lastErrorTimestamp == 0
                                        // 10分钟内闭嘴
                                        || System.currentTimeMillis() - lastErrorTimestamp > 1000 * 60 * 10) {
                                    sendError(e, batchId, offset, message);
                                    lastErrorTimestamp = System.currentTimeMillis();
                                }
                                Thread.sleep(500);
                            }
                        }
                    }
                }

            } catch (Throwable e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                logger.info("=============> Disconnect destination: {} <=============", this.name);
            }

            if (running) { // is reconnect
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    public ExecutorService getExecutorService() {
        if (executorService == null) {
            synchronized (this) {
                if (executorService == null) {
                    executorService = Util.newFixedThreadPool(adapterList.size(),
                            5000L, name + "-fork", false);
                }
            }
        }
        return executorService;
    }

    protected void writeOut(final List<Dml> message) {
        if (adapterList.size() == 1) {
            adapterList.get(0).sync(message);
            return;
        }

        List<Future<Boolean>> futures = new ArrayList<>();
        ExecutorService service = getExecutorService();
        // 组间适配器并行运行
        for (Adapter adapter : adapterList) {
            futures.add(service.submit(() -> {
                try {
                    long begin = System.currentTimeMillis();
                    adapter.sync(message);
                    if (logger.isTraceEnabled()) {
                        logger.trace("{} elapsed time: {}",
                                adapter.getClass().getName(),
                                (System.currentTimeMillis() - begin));
                    }
                    return true;
                } catch (Exception e) {
                    logger.error("writeOut 异常:{}", Util.getStackTrace(e));
                    throw e;
                }
            }));

            // 等待所有适配器写入完成
            // 由于是组间并发操作，所以将阻塞直到耗时最久的工作组操作完成
            Throwable exception = null;
            for (Future<Boolean> future : futures) {
                try {
                    if (!future.get()) {
                        exception = new RuntimeException("Outer adapter sync failed! ");
                    }
                } catch (Throwable e) {
                    exception = e;
                }
            }
            if (exception != null) {
                Util.sneakyThrows(exception);
            }
        }
    }

    public void start() {
        if (!running) {
            thread = this;
            setUncaughtExceptionHandler(handler);
            super.start();
            running = true;
        }
    }

    private void sendError(Throwable throwable,
                           String batchId,
                           String[] offset,
                           List<Dml> dmlList
    ) {
        String title = "ES搜索增量同步-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n name = " + name
                + ",\n\n batchId = " + batchId
                + ",\n\n offset = " + Arrays.toString(offset)
                + ",\n\n 异常 = " + throwable
                + ",\n\n SQL = " + (dmlList.isEmpty() ? null : SQL.DEFAULT_BUILDER.convert(dmlList.get(0)))
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    private void sendRecover(Throwable throwable, String batchId, String[] offset) {
        String title = "ES搜索增量同步-恢复正常";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n name = " + name
                + ",\n\n batchId = " + batchId
                + ",\n\n offset = " + Arrays.toString(offset);
        messageService.send(title, content);
    }

    private String messageID(List<Dml> message) {
        if (message.isEmpty()) {
            return null;
        }
        long min = message.stream().mapToLong(Dml::getLogfileOffset).min().getAsLong();
        long max = message.stream().mapToLong(Dml::getLogfileOffset).max().getAsLong();
        if (min == max) {
            return String.valueOf(min);
        } else {
            return min + "-" + max;
        }
    }

    @Override
    public String toString() {
        return name;
    }

    private String[] logfileOffset(List<Dml> message, int limit) {
        return message.stream().map(e -> e.getLogfileName() + "#" + e.getLogfileOffset()).limit(limit).toArray(String[]::new);
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void destroy0() {
        try {
            if (!running) {
                return;
            }

            running = false;

            logger.info("destination {} is waiting for adapters' worker thread die!", this.name);
            if (thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            if (executorService != null) {
                executorService.shutdown();
            }
            logger.info("destination {} adapters worker thread dead!", this.name);
            adapterList.forEach(Adapter::destroy);
            logger.info("destination {} all adapters destroyed!", this.name);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
