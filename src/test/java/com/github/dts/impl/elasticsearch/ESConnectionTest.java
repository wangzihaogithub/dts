package com.github.dts.impl.elasticsearch;

import com.github.dts.util.CanalConfig;
import com.github.dts.util.ESConnection;
import com.github.dts.util.EsTask;
import com.github.dts.util.EsTaskResponse;

import java.io.IOException;
import java.util.Collections;

public class ESConnectionTest {
    private static ESConnection esConnection(String address) {
        CanalConfig.OuterAdapterConfig.EsAccount esAccount = new CanalConfig.OuterAdapterConfig.Es();
        esAccount.setAddress(new String[]{address});
        return new ESConnection(esAccount);
    }

    public static void main(String[] args) throws IOException {
        ESConnection esConnection = esConnection("http://elasticsearch8.xx.com");

        EsTask task1 = esConnection.deleteByQuery("cnwy_job_test_index_v2",
                Collections.singletonMap("query", Collections.singletonMap("term", Collections.singletonMap("status", Collections.singletonMap("value", "3")))),
                null);
        task1.whenComplete((esTaskResponse, throwable) -> {
            long timestamp = task1.getTimestamp();
            long cost = System.currentTimeMillis() - timestamp;
            System.out.println("cost = " + cost + "ms");
        });

        EsTaskResponse join = task1.join();
        System.out.println("task = " + join);
    }
}
