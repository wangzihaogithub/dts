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
- 支持 es8写入
- 支持 es8的向量存储，dense_vector类型


### 代码例子 demo如下

https://github.com/wangzihaogithub/dts-demo


[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.wangzihaogithub/dts/badge.svg)](https://search.maven.org/search?q=g:com.github.wangzihaogithub%20AND%20a:dts)

```xml
<!-- https://github.com/wangzihaogithub/dts -->
<!-- https://mvnrepository.com/artifact/com.github.wangzihaogithub/dts -->
<dependency>
  <groupId>com.github.wangzihaogithub</groupId>
  <artifactId>dts</artifactId>
  <version>1.1.20</version>
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
        cnwy.llm-api-key: ${OPEN_API_KEY}
        canal.conf:
          enable-pull: true
          srcDataSources:
            defaultDS:
              url: 'jdbc:mysql://${MYSQL_HOST}:3306/cnwy?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&rewriteBatchedStatements=true&zeroDateTimeBehavior=CONVERT_TO_NULL'
              username: 'test_cnwy'
              password: ${MYSQL_PASSWORD}
          canalAdapters:
            - destination: 'cnwybinlog'
              topics:
                - 'cnwy\\.job.*'
                - 'cnwy\\.corp.*'
                - 'cnwy.region'
                - 'cnwy.kn_question'
                - 'cnwy.kn_scene'
              properties: {
                dataSource: 'defaultDS',
                maxDumpThread: 10,
                enableGTID: true
              }
              groups:
                - outerAdapters:
                    - name: 'adapterES716'
                      es7x:
                        concurrentBulkRequest: 4
                        address: 'https://my-elasticsearch-project-f10e79.es.ap-southeast-1.aws.elastic.cloud:443'
                        api-key: ${ES_API_KEY}
        spring:
          redis:
            host: ${REDIS_HOST}
            password: ${REDIS_PASSWORD}


          `


-  3.数据关系配置

        `
            dataSourceKey: defaultDS
            destination: cnwybinlog
            esMapping:
              env: dev
              pk: id
              _index: cnwy_job_test_index_alias
              upsert: false
              writeNull: false
              indexUpdatedTime: 'indexUpdatedTime'
              sql: "SELECT
                    job.id as id,
                    job.type as type,
                    job.education as education,
                    job.`name` as name,
                    job.notice_url as noticeUrl,
                    job.content as content,
                    job.content as contentVector,
                    job.`status` as status,
                    job.company_name as companyName,
                    job.create_time as createTime,
                    job.update_time as updateTime
                  FROM job job
                  "
              objFields:
                education:
                  type: array
                  paramArray:
                    split: '[、]'
                corp$aliasNames:
                  type: array
                  paramArray:
                    split: ','
                contentVector:
                  type: llm-vector
                  paramLlmVector:
                    apiKey: ${cnwy.llm-api-key}
                    baseUrl: https://dashscope.aliyuncs.com/compatible-mode/v1
                    modelName: text-embedding-v3
                    dimensions: 1024
                corp:
                  type: object-sql
                  paramSql:
                    sql: "SELECT
                            corp.id ,
                            corp.`name`,
                            GROUP_CONCAT(if(corpName.type = 2, corpName.`name`, null)) as aliasNames,
                            GROUP_CONCAT(if(corpName.type = 3, corpName.`name`, null)) as historyNames,
                            corp.group_name as groupCompany,
                            corp.data_type as dataType,
                            corp.central_country_ent_flag as centralCountryEntFlag
                          FROM corp corp
                          LEFT JOIN corp_name corpName on corpName.corp_id = corp.id 
                    "
                    onMainTableChangeWhereSql: 'WHERE corp.id = #{corp_id} '
                    onSlaveTableChangeWhereSql: 'WHERE corp.id = #{id} '
                corpTagList$tagIds:
                  type: array
                  paramArray:
                    split: ','
                corpTagList:
                  type: array-sql
                  paramSql:
                    sql: "SELECT
                            group_concat(corpRelationTag.tag_id) as tagIds,
                            corpTag.category_id as categoryId
                        FROM corp_relation_tag corpRelationTag
                        INNER JOIN corp_tag corpTag on corpTag.id = corpRelationTag.tag_id
                        GROUP BY corpRelationTag.corp_id, corpTag.category_id
              "
                    onMainTableChangeWhereSql: 'WHERE corpRelationTag.corp_id = #{corp_id} '
                    onSlaveTableChangeWhereSql: 'WHERE corpRelationTag.corp_id = #{corp_id} '
                regionList:
                  type: array-sql
                  paramSql:
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
                            concat(if(region.lat< region.lng, region.lat,region.lng), ',', if(region.lat > region.lng, region.lat,region.lng)) as geo
                        FROM job_region region "
                    onMainTableChangeWhereSql: 'WHERE region.job_id = #{id} '
                    onSlaveTableChangeWhereSql: 'WHERE region.job_id = #{job_id} '
                specialtyList:
                  type: array-sql
                  paramSql:
                    sql: "SELECT
                            specialty.id as id,
                            specialty.specialty_id as specialtyId,
                            specialty.specialty_name as specialtyName,
                            specialty.specialty_id_colloquial as specialtyIdColloquial,
                            specialty.specialty_name_colloquial as specialtyNameColloquial
                        FROM job_specialty specialty "
                    onMainTableChangeWhereSql: 'WHERE specialty.job_id = #{id} '
                    onSlaveTableChangeWhereSql: 'WHERE specialty.job_id = #{job_id} '
                corpRegionList:
                  type: array-sql
                  paramSql:
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
                             GROUP BY corpRegion.id
                  "
                    onMainTableChangeWhereSql: 'WHERE corpRegion.corp_id = #{corp_id} '
                    onSlaveTableChangeWhereSql: 'WHERE corpRegion.corp_id = #{corp_id} '


           
       `


 - 启动springboot 项目用mysql执行SQL： show processlist， 即可看到 binlog dump 线程已启动


### dts-sdk使用

```xml
<!-- https://github.com/wangzihaogithub/dts-sdk -->
<!-- https://mvnrepository.com/artifact/com.github.wangzihaogithub/dts-sdk -->
<dependency>
  <groupId>com.github.wangzihaogithub</groupId>
  <artifactId>dts-sdk</artifactId>
  <version>1.1.20</version>
</dependency>
```

-  1.仅导入上面的maven包就行

   yaml配置

        server:
          dts:
            sdk:
              cluster:
                discovery: redis
        spring:
          redis:
            host: localhost
            password: xxx
    

        @Autowired
        private DtsSdkClient dtsSdkClient;
        @PostMapping("/save")
        public CompletableFuture<AjaxResult> save(@RequestBody OrderRequest request) {
            Integer orderId = orderService.save(request);
            return dtsSdkClient.listenEs("cnwy_order_test_index_alias", orderId, 500)
                    .handle((listenEsResponse, throwable) -> AjaxResult.success(orderId));
        }

