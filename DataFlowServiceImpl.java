package com.xxx.xxx.etl.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataFlowServiceImpl implements IDataFlowService {

    private final IAssignmentConfigFieldService configFieldService;
    private final TbMasterDataMapper tbMasterDataMapper;
    private final ITaskExecuteService taskExecuteService;

    private static final String tableType = HoodieTableType.MERGE_ON_READ.name();
    private static String TABLEPATH = "hdfs://ip:port/user/hive/warehouse/hudi_etl/";
    
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public DataFlowServiceImpl(IAssignmentConfigFieldService configFieldService, TbMasterDataMapper tbMasterDataMapper, ITaskExecuteService taskExecuteService) {
        this.configFieldService = configFieldService;
        this.tbMasterDataMapper = tbMasterDataMapper;
        this.taskExecuteService = taskExecuteService;
    }

    @Override
    @Async
    public void specifyTaskDetail(AssignmentConfigEntity task) {
        // get Hbase column
        log.info("specifyTaskDetail for taskId = {} begin!!!", task.getId());
        List<AssignmentConfigFieldAddDTO> fieldList = configFieldService.getByParentId(task.getId());
        String taskUuid = UUID.randomUUID().toString();

        //generate schema
        Schema schema = generateSchema(fieldList);
        log.info("taskId = {}\nschema: {}", task.getId(), schema);

        HBaseOperateVO hbaseVo = createHBaseOperateVO(task, fieldList, schema);
        HoodieOperateVO hoodieVo = createHoodieOperateVO(task, schema);

        //记录任务开始，修改任务状态
        TaskExecuteVO executeVO = initializeTaskExecuteVO(task, taskUuid);
        this.changeTaskStatus(executeVO);

        this.writeDataFromHBase2Hoodie(hbaseVo, hoodieVo, executeVO);

        executeVO.setExecuteState(2);//执行完成
        executeVO.setCompleteTime(new Date());
        this.changeTaskStatus(executeVO);
        log.info("specifyTaskDetail function is completed!!!");
    }

    private Schema generateSchema(List<AssignmentConfigFieldAddDTO> fieldList) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("rowKey", Schema.create(Schema.Type.STRING), null, null));

        for (AssignmentConfigFieldAddDTO dto : fieldList) {
            String columnName = dto.getColumnFamilyField();
            fields.add(new Schema.Field(columnName, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), null, null));
        }
        return Schema.createRecord("HBaseDataRecord", null, null, false, fields);
    }

    private HBaseOperateVO createHBaseOperateVO(AssignmentConfigEntity task, List<AssignmentConfigFieldAddDTO> fieldList, Schema schema) {
        HBaseOperateVO hbaseVo = new HBaseOperateVO();
        hbaseVo.setTaskId(task.getId());
        hbaseVo.setTableName(task.getSourceTableName());
        hbaseVo.setSchema(schema);
        hbaseVo.setStartTime(task.getSourceRowkeyStart());
        hbaseVo.setEndTime(task.getSourceRowkeyEnd());
        hbaseVo.setConfigFieldAddDTOList(fieldList);
        return hbaseVo;
    }

    private HoodieOperateVO createHoodieOperateVO(AssignmentConfigEntity task, Schema schema) {
        HoodieOperateVO hoodieVo = new HoodieOperateVO();
        hoodieVo.setSchema(schema);
        hoodieVo.setTableName(task.getTargetTableName());
        hoodieVo.setTablePath(TABLEPATH + task.getTargetDatasourceName() + "/" + task.getTargetTableName() + "/");
        return hoodieVo;
    }

    private TaskExecuteVO initializeTaskExecuteVO(AssignmentConfigEntity task, String taskUuid) {
        TaskExecuteVO executeVO = new TaskExecuteVO();
        executeVO.setTaskId(task.getId());
        executeVO.setTaskUuid(taskUuid);
        executeVO.setExecuteState(1); // Executing
        executeVO.setExecuteTime(new Date());
        executeVO.setDeburring(task.getDeburring());////当前任务是否需要去毛刺
        return executeVO;
    }

    private boolean changeTaskStatus(TaskExecuteVO executeVO) {
        LambdaQueryWrapper<TaskExecuteEntity> wrapper = Wrappers.<TaskExecuteEntity>lambdaQuery()
                .eq(TaskExecuteEntity::getTaskId, executeVO.getTaskId())
                .eq(TaskExecuteEntity::getTaskUuid, executeVO.getTaskUuid());
        List<TaskExecuteEntity> list = taskExecuteService.list(wrapper);

        TaskExecuteEntity entity = new TaskExecuteEntity();
        entity.setTaskUuid(executeVO.getTaskUuid());
        entity.setTaskId(executeVO.getTaskId());
        entity.setExecuteState(executeVO.getExecuteState());
        entity.setExecuteTime(executeVO.getExecuteTime());

        if (CollectionUtils.isEmpty(list)) {
            //保存一条新的记录
            return taskExecuteService.save(entity);
        } else {
            //更新状态
            entity.setId(list.get(0).getId());
            entity.setCompleteTime(executeVO.getCompleteTime());
            return taskExecuteService.updateById(entity);
        }
    }

    public void writeDataFromHBase2Hoodie(HBaseOperateVO hbaseVo, HoodieOperateVO hoodieVo, TaskExecuteVO executeVO) {
        Connection connection = HBaseConnection.getConnection();
        TableName tableName = TableName.valueOf(hbaseVo.getTableName());
        // 获取表的所有列族 HBase一个列族一个列族分开获取对性能比较好
        List<String> columnFamilies = getColumnFamilies(connection, tableName);
        List<byte[]> rowkeyRanges = this.splitRowkeyForMultiTask(hbaseVo.getStartTime(), hbaseVo.getEndTime());

        //若表不存在先创建表
        this.createHoodieTableIfNotExist(hoodieVo);
        //每次任务都创建一个新的HBase连接，记得关闭资源
        Configuration config = HBaseConfiguration.create();
        try (Connection connEachTask = ConnectionFactory.createConnection(config)) {
            for (int i = 0; i < rowkeyRanges.size(); i += 2) {
                byte[] startRow = rowkeyRanges.get(i);
                byte[] endRow = rowkeyRanges.get(i + 1);
                List<HoodieRecord<HoodieAvroPayload>> records = this.processHBaseData(connEachTask, tableName, columnFamilies, startRow, endRow, hbaseVo, executeVO);
                this.writeIntoHudi(hoodieVo, records);
            }
        } catch (IOException e) {
            log.error("获取HBase连接失败");
            log.error(e.getMessage());
        }
    }

    /**
     * get data from hbase and generate hoodie data
     */
    private List<HoodieRecord<HoodieAvroPayload>> processHBaseData(Connection connection, TableName tableName, List<String> columnFamilies, byte[] startRow, byte[] endRow, HBaseOperateVO hbaseVo, TaskExecuteVO executeVO) {

        List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
        try (Table table = connection.getTable(tableName)) {
            Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
            for (AssignmentConfigFieldAddDTO addDTO : hbaseVo.getConfigFieldAddDTOList()) {
                scan.addColumn(addDTO.getColumnFamily().getBytes(), addDTO.getColumnFamilyField().getBytes());
            }
            try (ResultScanner newScanner = table.getScanner(scan)) {
                for (Result result : newScanner) {
                    GenericData.Record record = new GenericData.Record(hbaseVo.getSchema());
                    record.put("rowKey", new String(result.getRow()));
                    for (String family : columnFamilies) {
                        byte[] familyBytes = Bytes.toBytes(family);
                        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(familyBytes);
                        for (Map.Entry<byte[], byte[]> columnEntry : familyMap.entrySet()) {
                            String value = new String(columnEntry.getValue());
                            record.put(columnEntry.getKey(), value);
                        }
                    }
                    this.setHoodieRecordIntoList(record, result, records);
                }
            } catch (Exception e) {
                log.error("获取数据出错：{}", new String(scan.getStartRow()));
                log.error(e.getMessage());
                executeVO.setExecuteState(3);//执行失败
                executeVO.setCompleteTime(new Date());
                this.changeTaskStatus(executeVO);
            }
        } catch (Exception e) {
            log.error("获取HBase表失败");
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return records;
    }

    private void setHoodieRecordIntoList(GenericData.Record record, Result result, List<HoodieRecord<HoodieAvroPayload>> records) {
        HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(record));
        String partCol = new String(result.getRow());
        // 将字符串解析为LocalDateTime对象
        LocalDateTime dateTime = LocalDateTime.parse(partCol, formatter);
        //2024-10-12 修改，按照年份+月份分区
        // String partitionPath = dateTime.toLocalDate().toString(); // 提取LocalDate部分 默认格式为yyyy-MM-dd 将LocalDate转换回字符串
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
        String partitionPath = dateTime.format(outputFormatter);

        String recordKey = partCol.replace(' ', '_').replace(':', '-');//转换recordKey为该字段要求合法字符串
        HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
        HoodieRecord<HoodieAvroPayload> hoodieRecord = new HoodieAvroRecord<HoodieAvroPayload>(hoodieKey, payload);
        records.add(hoodieRecord);
    }

    private List<String> getColumnFamilies(Connection connection, TableName tableName) {
        List<String> columnFamilies = new ArrayList<>();
        try (Admin admin = connection.getAdmin()) {
            TableDescriptor descriptor = admin.getDescriptor(tableName);
            for (ColumnFamilyDescriptor familyDescriptor : descriptor.getColumnFamilies()) {
                columnFamilies.add(familyDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("获取表 {} 的列族信息时出错", tableName, e);
            throw new RuntimeException("获取列族信息失败", e);
        }
        return columnFamilies;
    }

    private void createHoodieTableIfNotExist(HoodieOperateVO entity) {
        String tablePath = entity.getTablePath();
        String tableName = entity.getTableName();
        Configuration hadoopConf = new Configuration();
        Path path = new Path(entity.getTablePath());
        try {
            FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
            log.info("Resolved filesystem for path {}: {}", tablePath, fs.getUri());
            if (!fs.exists(path)) {
                log.info("Path does not exist: {}. Creating...", tablePath);
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType)
                        .setTableName(tableName)
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .initTable(hadoopConf, tablePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }

    /**
     * using java client write into hudi
     */
    private void writeIntoHudi(HoodieOperateVO entity, List<HoodieRecord<HoodieAvroPayload>> records) {
        String tablePath = entity.getTablePath();
        String tableName = entity.getTableName();
        Configuration hadoopConf = new Configuration();
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(entity.getSchema().toString()).withParallelism(10, 10)
                .withDeleteParallelism(10).forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(40, 50).build()).build();
        HoodieJavaWriteClient<HoodieAvroPayload> hoodieWriteClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

        try {
            String newCommitTime = hoodieWriteClient.startCommit();
            List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
            List<HoodieRecord<HoodieAvroPayload>> writeRecords =
                    recordsSoFar.stream().map(r -> new HoodieAvroRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
            hoodieWriteClient.upsert(writeRecords, newCommitTime);
        } catch (Exception e) {
            log.error("Hudi 写入失败");
            log.error(e.getMessage());
            e.printStackTrace();
        } finally {
            hoodieWriteClient.close();
        }
    }

    private List<byte[]> splitRowkeyForMultiTask(Date beginRowkey, Date endRowkey) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = beginRowkey.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime endTime = endRowkey.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

        List<byte[]> rowKeyRanges = new ArrayList<>();
        LocalDateTime current = startTime;
        while (current.isBefore(endTime)) {
            rowKeyRanges.add(Bytes.toBytes(current.format(formatter)));
            LocalDateTime next = current.plusHours(6);
            rowKeyRanges.add(Bytes.toBytes(next.format(formatter)));
            current = next;
        }
        return rowKeyRanges;
    }

}
