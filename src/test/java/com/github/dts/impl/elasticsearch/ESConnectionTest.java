package com.github.dts.impl.elasticsearch;

import com.github.dts.util.CanalConfig;
import com.github.dts.util.ESConnection;
import com.github.dts.util.EsTask;
import com.github.dts.util.EsTaskResponse;

import java.io.IOException;
import java.util.function.BiConsumer;

public class ESConnectionTest {
    public static void main(String[] args) throws IOException {
        CanalConfig.OuterAdapterConfig.EsAccount esAccount = new CanalConfig.OuterAdapterConfig.Es();
        esAccount.setAddress(new String[]{"http://elasticsearch8.xxx.com"});
        ESConnection esConnection = new ESConnection(esAccount);

        EsTaskResponse task = esConnection.tasksGet("no2FpGyIQmefCE7973Wz4A:659147103");
        EsTask task1 = esConnection.forcemerge("cnwy_job_test_index_v2", 1, null);
        task1.whenComplete(new BiConsumer<EsTaskResponse, Throwable>() {
            @Override
            public void accept(EsTaskResponse esTaskResponse, Throwable throwable) {
                long timestamp = task1.getTimestamp();
                long cost = System.currentTimeMillis() - timestamp;
                System.out.println("cost = " + cost + "ms");
            }
        });

        EsTaskResponse join = task1.join();
        System.out.println("task = " + join);
    }
}
