package com.github.dts.impl.rds;

import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * RDS外部适配器
 */
public class RDSAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(RDSAdapter.class);
    private CanalConfig.OuterAdapterConfig configuration;

    @Override
    public void init(CanalConfig.OuterAdapterConfig configuration, Properties envProperties) {
        this.configuration = configuration;
    }

    @Override
    public void sync(List<Dml> dmls, MetaDataRepository.Acknowledge acknowledge) {
        acknowledge.ack();
        List<SQL> sqlList = SQL.DEFAULT_BUILDER.convert(dmls.stream().limit(20).collect(Collectors.toList()));
        log.info("rds {}", sqlList);
    }

    @Override
    public void destroy() {

    }

    @Override
    public CanalConfig.OuterAdapterConfig getConfiguration() {
        return configuration;
    }
}
