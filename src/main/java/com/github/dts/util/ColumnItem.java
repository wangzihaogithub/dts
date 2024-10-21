package com.github.dts.util;

import java.util.Objects;

public class ColumnItem {
    private String owner;
    private String columnName;

    public static ColumnItem parse(String e) {
        String[] split = e.split("[.]");
        ColumnItem columnItem = new ColumnItem();
        if (split.length == 1) {
            columnItem.setColumnName(split[0]);
        } else {
            columnItem.setOwner(split[0]);
            columnItem.setColumnName(split[1]);
        }
        return columnItem;
    }

    public boolean equalsColumnItem(ColumnItem that) {
        if (this == that) return true;
        return Objects.equals(owner, that.owner) && Objects.equals(columnName, that.columnName);
    }

    @Override
    public String toString() {
        return Objects.toString(owner, "") + "." + columnName;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}