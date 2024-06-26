# dts

#### 介绍
数据同步 
- 延迟低至5ms（修改数据库至elasticsearch可以搜索到）
- 支持 同步elasticsearch-Nested字段，加自定义字段，解析url转换为文本
- 支持 直连数据库binglog
- 支持 连阿里云-kafka-binlog
- 支持 自定义监听
- 支持 将Row变更转化为SQL语句对象
- 支持 报警消息


[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.wangzihaogithub/dts/badge.svg)](https://search.maven.org/search?q=g:com.github.wangzihaogithub%20AND%20a:dts)

```xml
<!-- https://github.com/wangzihaogithub/dts -->
<!-- https://mvnrepository.com/artifact/com.github.wangzihaogithub/dts -->
<dependency>
  <groupId>com.github.wangzihaogithub</groupId>
  <artifactId>dts</artifactId>
  <version>1.1.1</version>
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
        cnwy.kafka-prefix: 'dev_'
        cnwy.binlog-prefix: 'cnwy.'
        canal.conf:
            srcDataSources:
                defaultDS:
                    url: 'jdbc:mysql://xx:3306/xx?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true&zeroDateTimeBehavior=CONVERT_TO_NULL'
                    username: 'xx'
                    password: 'xx'
          - destination: 'job_es'
            connector: com.github.dts.canal.MysqlBinlogCanalConnector
            topics:
              - '${cnwy.binlog-prefix}job'
              - '${cnwy.binlog-prefix}job_region'
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
           destination: job_es
           esMapping:
               env: prod
               _id: id
               pk: id
               _index: cnwy_job_prod_index_alias
               mappingMetadataTimeout: 600000
               upsert: false
               writeNull: false
               indexUpdatedTime: 'indexUpdatedTime'
               sql: "SELECT
                   job.id as id,
                   job.type as type,
                   job.`name` as name,
                   job.job_start_time as jobStartTime,
                   job.job_end_time as jobEndTime,
                   job.`status` as status,
                   job.company_id as companyId,
                   job.company_name as companyName,
                   substring_index(job.company_name,'-', 1) as shortCompanyName,
                   job.create_time as createTime,
                   job.update_time as updateTime
                   FROM job job"
               objFields:
                   education:
                       type: array
                       split: '[、]'
                   regionList:
                       type: array-sql
                       sql: "SELECT
                           region.id as id,
                           region.region_id as regionId,
                           region.province_id as provinceId,
                           region.province_name as provinceName,
                           region.city_id as cityId,
                           region.city_name as cityName,
                           region.district_id as districtId,
                           region.district_name as districtName,
                           region.address as address,
                           region.region_id_colloquial as regionIdColloquial,
                           region.region_name_colloquial as regionNameColloquial,
                           concat(region.lat, ',', region.lng) as geo
                           FROM job_region region "
                       onParentChangeWhereSql: 'WHERE region.id = #{id} '
                       onChildChangeWhereSql: 'WHERE region.job_id = #{job_id} '
                       parentDocumentId: job_id

       `


 - 启动springboot 项目用mysql执行SQL： show processlist， 即可看到 binlog dump 线程已启动

