package com.github.dts.util;

import org.slf4j.Logger;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class Watch {
    private final String id;
    private boolean keepTaskListFlag;
    private final List<TaskInfo> taskList;
    private long startTimeMillis;
    private boolean running;
    private String currentTaskName;
    private TaskInfo lastTaskInfo;
    private int taskCount;
    private long totalTimeMillis;
    private Date watchCreateTime;
    private Consumer<String> printStream;

    public Watch() {
        this("", System.out::println);
    }

    public Watch(String id, PrintStream printStream) {
        this(id, printStream::println);
    }

    public Watch(Logger logger) {
        this("", logger::info);
    }

    public Watch(String id, Logger logger) {
        this(id, logger::info);
    }

    public Watch(String id, java.util.logging.Logger logger) {
        this(id, logger::info);
    }

    public Watch(Consumer<String> printStream) {
        this("", printStream);
    }

    public Watch(String id, Consumer<String> printStream) {
        this.printStream = printStream;
        this.keepTaskListFlag = false;
        this.taskList = new LinkedList<>();
        this.id = id;
        this.watchCreateTime = new Date();
    }

    private void startAfter() throws IllegalStateException {
        printStream.accept(" 任务 : 启动 -> " + id + currentTaskName() + "");
    }

    private void stopAfter() throws IllegalStateException {
        TaskInfo info = getLastTaskInfo();
        printStream.accept(" 任务 : 停止 , 时长 : " + info.getTimeSeconds() + "秒 -> (" + id + info.getTaskName() + ")");
    }

    private void errorAfter(String message) throws IllegalStateException {
        printStream.accept(" 任务 : 发生错误 -> " + id + currentTaskName() + " \r\n [" + message + "]");
    }


    public String getId() {
        return this.id;
    }

    public void setKeepTaskListFlag(boolean keepTaskListFlag) {
        this.keepTaskListFlag = keepTaskListFlag;
    }

    public void start(String taskName) throws IllegalStateException {
        if (this.running) {
            throw new IllegalStateException("Can't start Watch: it's already running");
        } else {
            this.running = true;
            this.currentTaskName = taskName;
            this.startTimeMillis = System.currentTimeMillis();
        }
        startAfter();
    }

    public void stop() throws IllegalStateException {
        if (!this.running) {
            throw new IllegalStateException("Can't stop Watch: it's not running");
        } else {
            long lastTime = System.currentTimeMillis() - this.startTimeMillis;
            this.totalTimeMillis += lastTime;
            this.lastTaskInfo = new TaskInfo(this.currentTaskName, lastTime);
            if (this.keepTaskListFlag) {
                this.taskList.add(this.lastTaskInfo);
            }

            ++this.taskCount;
            this.running = false;
            this.currentTaskName = null;
        }
        stopAfter();
    }

    public void error(String message) throws IllegalStateException {
        errorAfter(message);
    }

    public boolean isRunning() {
        return this.running;
    }

    public String currentTaskName() {
        return this.currentTaskName;
    }

    public long getLastTaskTimeMillis() throws IllegalStateException {
        if (this.lastTaskInfo == null) {
            throw new IllegalStateException("No tasks run: can't get last task interval");
        } else {
            return this.lastTaskInfo.getTimeMillis();
        }
    }

    public String getLastTaskName() throws IllegalStateException {
        if (this.lastTaskInfo == null) {
            throw new IllegalStateException("No tasks run: can't get last task name");
        } else {
            return this.lastTaskInfo.getTaskName();
        }
    }

    public TaskInfo getLastTaskInfo() throws IllegalStateException {
        if (this.lastTaskInfo == null) {
            throw new IllegalStateException("No tasks run: can't get last task info");
        } else {
            return this.lastTaskInfo;
        }
    }

    public long getTotalTimeMillis() {
        return this.totalTimeMillis;
    }

    public double getTotalTimeSeconds() {
        return (double) this.totalTimeMillis / 1000.0D;
    }

    public int getTaskCount() {
        return this.taskCount;
    }

    public TaskInfo[] getTaskInfo() {
        if (!this.keepTaskListFlag) {
            throw new UnsupportedOperationException("Task info is not being kept!");
        } else {
            return (TaskInfo[]) this.taskList.toArray(new TaskInfo[this.taskList.size()]);
        }
    }

    public String shortSummary() {
        return "Watch '" + this.getId() + "': running time (millis) = " + this.getTotalTimeMillis();
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder(this.shortSummary());
        sb.append('\n');
        if (!this.keepTaskListFlag) {
            sb.append("No task info kept");
        } else {
            sb.append("-----------------------------------------\n");
            sb.append("ms     %     Task name\n");
            sb.append("-----------------------------------------\n");
            NumberFormat nf = NumberFormat.getNumberInstance();
            nf.setMinimumIntegerDigits(5);
            nf.setGroupingUsed(false);
            NumberFormat pf = NumberFormat.getPercentInstance();
            pf.setMinimumIntegerDigits(3);
            pf.setGroupingUsed(false);
            TaskInfo[] var4 = this.getTaskInfo();
            int var5 = var4.length;

            for (int var6 = 0; var6 < var5; ++var6) {
                TaskInfo task = var4[var6];
                sb.append(nf.format(task.getTimeMillis())).append("  ");
                sb.append(pf.format(task.getTimeSeconds() / this.getTotalTimeSeconds())).append("  ");
                sb.append(task.getTaskName()).append("\n");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.shortSummary());
        if (this.keepTaskListFlag) {
            TaskInfo[] var2 = this.getTaskInfo();
            int var3 = var2.length;

            for (int var4 = 0; var4 < var3; ++var4) {
                TaskInfo task = var2[var4];
                sb.append("; [").append(task.getTaskName()).append("] took ").append(task.getTimeMillis());
                long percent = Math.round(100.0D * task.getTimeSeconds() / this.getTotalTimeSeconds());
                sb.append(" = ").append(percent).append("%");
            }
        } else {
            sb.append("; no task info kept");
        }

        return sb.toString();
    }

    public Date getWatchCreateTime() {
        return watchCreateTime;
    }

    public static final class TaskInfo {
        private final String taskName;
        private final long timeMillis;

        TaskInfo(String taskName, long timeMillis) {
            this.taskName = taskName;
            this.timeMillis = timeMillis;
        }

        public String getTaskName() {
            return this.taskName;
        }

        public long getTimeMillis() {
            return this.timeMillis;
        }

        public double getTimeSeconds() {
            return (double) this.timeMillis / 1000.0D;
        }
    }
}
