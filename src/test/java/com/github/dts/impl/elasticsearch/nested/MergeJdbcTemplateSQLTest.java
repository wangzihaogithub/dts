package com.github.dts.impl.elasticsearch.nested;

import com.github.dts.util.ColumnItem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class MergeJdbcTemplateSQLTest {
    public static void main(String[] args) {
        JdbcTemplateSQL sql = new JdbcTemplateSQL(
                "select j.id ,j.name from job j where id = ? order by j.id limit 100",
                new Object[]{1},
                Collections.emptyMap(),
                "",
                Collections.singletonList(ColumnItem.parse("j.id"))
        );
        JdbcTemplateSQL sql1 = new JdbcTemplateSQL(
                "select j.id ,j.name from job j where id = ? order by j.id limit 100",
                new Object[]{2},
                Collections.emptyMap(),
                "",
                Collections.singletonList(ColumnItem.parse("j.id"))
        );
        Collection<MergeJdbcTemplateSQL<JdbcTemplateSQL>> merge = MergeJdbcTemplateSQL.merge(Arrays.asList(sql, sql1), 100);

        System.out.println("merge = " + merge);
    }
}
