package disruptor;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlMultiStageCoprocessor;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    public static void main(String[] args) throws Exception {
        RingBuffer<LongEvent> ringBuffer = t(1024);

        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
            System.out.println("l = " + l);
//            Thread.sleep(1000);
        }
    }

    private static void t1(int bufferSize) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Disruptor<LongEvent> disruptor =
                new Disruptor<>(LongEvent::new, bufferSize, executorService);

        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            System.out.println("Event: " + event);
            Thread.sleep(1000);
        })
        ;
        disruptor.start();


        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
            System.out.println("l = " + l);
//            Thread.sleep(1000);
        }
    }

    private static RingBuffer<LongEvent> t(int ringBufferSize) {
        RingBuffer<LongEvent> disruptorMsgBuffer = RingBuffer.createSingleProducer(LongEvent::new,
                ringBufferSize,
                new BlockingWaitStrategy());
        int tc = 4;
        ExecutorService parserExecutor = Executors.newFixedThreadPool(tc, new NamedThreadFactory("MultiStageCoprocessor-Parser-"));

        ExecutorService stageExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("MultiStageCoprocessor-other-"));
        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        ExceptionHandler<? super LongEvent> exceptionHandler = new ExceptionHandler<LongEvent>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, LongEvent event) {
                throw new CanalParseException(ex);
            }

            @Override
            public void handleOnStartException(Throwable ex) {

            }

            @Override
            public void handleOnShutdownException(Throwable ex) {

            }
        };
        // stage 2
        BatchEventProcessor simpleParserStage = new BatchEventProcessor<>(disruptorMsgBuffer,
                sequenceBarrier,
                new EventHandler<LongEvent>() {
                    @Override
                    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {
                        System.out.println("stage 2 BatchEventProcessor = " + event);

                        Thread.sleep(1000);
                    }
                });
        simpleParserStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(simpleParserStage.getSequence());

        // stage 3
        SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier(simpleParserStage.getSequence());
        WorkHandler<? super LongEvent>[] workHandlers = new WorkHandler[tc];
        for (int i = 0; i < tc; i++) {
            workHandlers[i] = new WorkHandler<LongEvent>() {
                @Override
                public void onEvent(LongEvent event) throws Exception {
                    System.out.println("WorkHandler = " + event);
                }
            };
        }
        WorkerPool<LongEvent> workerPool = new WorkerPool<LongEvent>(disruptorMsgBuffer,
                dmlParserSequenceBarrier,
                exceptionHandler,
                workHandlers);
        Sequence[] sequence = workerPool.getWorkerSequences();
        disruptorMsgBuffer.addGatingSequences(sequence);

        // stage 4
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        BatchEventProcessor<LongEvent> sinkStoreStage = new BatchEventProcessor<>(disruptorMsgBuffer, sinkSequenceBarrier, new EventHandler<LongEvent>() {
            @Override
            public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {
                System.out.println("stage 4 BatchEventProcessor = " + event);
            }
        });
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(simpleParserStage);
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
        return disruptorMsgBuffer;
    }

    public static class LongEvent {
        private long value;

        public void set(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "LongEvent{" + "value=" + value + '}';
        }
    }
}
