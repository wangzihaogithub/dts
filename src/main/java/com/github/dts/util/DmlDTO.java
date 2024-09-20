package com.github.dts.util;

import java.util.List;
import java.util.Map;

public class DmlDTO {
    private String tableName;
    private String database;
    private List<String> pkNames;
    private Long es;
    private Long ts;
    private String type;
    private Map<String, Object> old;
    private Map<String, Object> data;
    private List<Dependent> dependents;
    private String adapterName;

    public String getAdapterName() {
        return adapterName;
    }

    public void setAdapterName(String adapterName) {
        this.adapterName = adapterName;
    }

    public List<Dependent> getDependents() {
        return dependents;
    }

    public void setDependents(List<Dependent> dependents) {
        this.dependents = dependents;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public static class Dependent {
        private String name;
        private Boolean effect;
        private String esIndex;

        @Override
        public String toString() {
            return "Dependent{" +
                    "name='" + name + '\'' +
                    ", effect=" + effect +
                    ", esIndex='" + esIndex + '\'' +
                    '}';
        }

        public String getEsIndex() {
            return esIndex;
        }

        public void setEsIndex(String esIndex) {
            this.esIndex = esIndex;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getEffect() {
            return effect;
        }

        public void setEffect(Boolean effect) {
            this.effect = effect;
        }
    }
}
