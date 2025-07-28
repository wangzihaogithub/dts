package com.github.dts.impl.elasticsearch;

import com.github.dts.util.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

public class ESConnectionIndexTest {
    private static ESConnection esConnection(String address) {
        CanalConfig.OuterAdapterConfig.EsAccount esAccount = new CanalConfig.OuterAdapterConfig.Es();
        esAccount.setAddress(new String[]{address});
        return new ESConnection(esAccount);
    }

    public static void main(String[] args) throws IOException {
        ESConnection esConnection = esConnection("http://elasticsearch8.xx.com");

        DefaultESTemplate esTemplate = new DefaultESTemplate(esConnection);
        String yyyyMmDd = new SimpleDateFormat("yyyy_MM_dd").format(new Date());
        String aliasIndexName = "cnwy_kn_employees_test";
        String newIndexName = aliasIndexName + "_" + yyyyMmDd;
        CompletableFuture<EsActionResponse> reindex = esTemplate.reindex(aliasIndexName, newIndexName);
        EsActionResponse join = reindex.join();

        System.out.println("join = " + join);
    }

}
