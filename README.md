# dts

#### 介绍
数据同步 
- 延迟低至5ms（修改数据库至elasticsearch可以搜索到）
- 支持 同步elasticsearch-Nested字段，会收集sql的join相关表，自动反向更新，支持自定义处理字段，解析url转换为文本
- 支持 直连数据库binglog，支持redis记忆offset，不丢binlog
- 支持 连阿里云-kafka-binlog
- 支持 自定义监听
- 支持 将Row变更转化为SQL语句对象
- 支持 报警消息


### 代码例子 demo如下

https://github.com/wangzihaogithub/dts-demo


[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.wangzihaogithub/dts/badge.svg)](https://search.maven.org/search?q=g:com.github.wangzihaogithub%20AND%20a:dts)

```xml
<!-- https://github.com/wangzihaogithub/dts -->
<!-- https://mvnrepository.com/artifact/com.github.wangzihaogithub/dts -->
<dependency>
  <groupId>com.github.wangzihaogithub</groupId>
  <artifactId>dts</artifactId>
  <version>1.1.9</version>
</dependency>
```
    
-  1.仅导入上面的maven包就行

    
        @SpringBootApplication
        public class Application {
            public static void main(String[] args) {
                SpringApplication.run(Application.class, args);
            }
        }


- 2.application-dev.yaml 配置

        `
        canal.conf:
            srcDataSources:
                defaultDS:
                    url: 'jdbc:mysql://xx:3306/xx?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true&zeroDateTimeBehavior=CONVERT_TO_NULL'
                    username: 'xx'
                    password: 'xx'
          - destination: 'cnwybinlog'
            connector: com.github.dts.canal.MysqlBinlogCanalConnector
            topics:
              - 'cnwy\\.job.*'
              - 'cnwy\\.corp.*'
              - 'cnwy.region'
            properties: {
                dataSource: 'defaultDS'
            }
            groups:
              - outerAdapters:
                  - name: 'adapterES7'
                    es7x:
                        address: 'xx.com:9200'
                        username: 'xx'
                        password: 'xx'

          `


-  3.数据关系配置

        `
        dataSourceKey: defaultDS
        destination: cnwybinlog
        esMapping:
          env: test
          _id: id
          pk: id
          _index: cnwy_corp_test_index_alias
          mappingMetadataTimeout: 600000
          upsert: false
          writeNull: false
          sql: "SELECT
            corp.id ,
            corp.`name`,
            GROUP_CONCAT(if(corpName.type = 2, corpName.`name`, null)) as aliasNames,
            corp.create_time as createTime
            FROM corp corp
            LEFT JOIN corp_name corpName on corpName.corp_id = corp.id "
          objFields:
            aliasNames:
              type: array
              split: ','
            regionList:
              type: array-sql
              sql: "SELECT
                        corpRegion.province_id as provinceId,
                        corpRegion.city_id as cityId,
                        corpRegion.district_id as districtId,
                        corpRegion.region_id as regionId,
                        province.`name` as provinceName,
                        city.`name` as cityName,
                        district.`name` as districtName,
                        concat(if(region.lat< region.lng, region.lat,region.lng), ',', if(region.lat > region.lng, region.lat,region.lng)) as geo
                    FROM corp_region corpRegion
                    LEFT JOIN region region on region.id = corpRegion.region_id
                    LEFT JOIN region province on province.id = corpRegion.province_id
                    LEFT JOIN region city on city.id = corpRegion.city_id
                    LEFT JOIN region district on district.id = corpRegion.district_id
         "
              onParentChangeWhereSql: 'WHERE corpRegion.corp_id = #{id} '
              onChildChangeWhereSql: 'WHERE corpRegion.corp_id = #{job_id} '
            tagList:
              type: array-sql
              sql: "SELECT
                      corpRelationTag.tag_id as tagId,
                      corpTag.`name` as tagName,
                      corpTag.category_id as categoryId,
                      corpCategory.`name` as categoryName,
                      corpCategory.sort as categorySort,
                      corpCategory.`status` as categoryStatus,
                      corpTag.source_enum as tagSource,
                      corpTag.`status` as tagStatus,
                      corpTag.change_flag as tagChangeFlag
                  FROM corp_relation_tag corpRelationTag
                  INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id
                  LEFT JOIN corp_category corpCategory on corpCategory.id = corpTag.category_id
        "
              onParentChangeWhereSql: 'WHERE corpRelationTag.corp_id = #{id} '
              onChildChangeWhereSql: 'WHERE corpRelationTag.corp_id = #{corp_id} '

           
       `


 - 启动springboot 项目用mysql执行SQL： show processlist， 即可看到 binlog dump 线程已启动

