package com.github.dts.cluster;

public enum MessageTypeEnum {
    ES_DML("es-dml"),
    RDS_SQL("rds-sql");
    private final String type;

    MessageTypeEnum(String type) {
        this.type = type;
    }

    public static MessageTypeEnum getByType(String type) {
        for (MessageTypeEnum value : values()) {
            if (value.getType().equals(type)) {
                return value;
            }
        }
        return null;
    }

    public String getType() {
        return type;
    }
}