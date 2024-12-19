package com.baosteel.bsee.etl.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;

@Slf4j
public class Test12485 {

    public static final String DEFAULT_FIRST_PARTITION_PATH = "2020/01/01";
    public static final String DEFAULT_SECOND_PARTITION_PATH = "2020/01/02";
    public static final String DEFAULT_THIRD_PARTITION_PATH = "2020/01/03";

    public static final String[] DEFAULT_PARTITION_PATHS =
            {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};

    public static String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ "
            + "{\"name\": \"ts\",\"type\": \"long\"},{\"name\": \"uuid\", \"type\": \"string\"},"
            + "{\"name\": \"rider\", \"type\": \"string\"},{\"name\": \"driver\", \"type\": \"string\"},"
            + "{\"name\": \"begin_lat\", \"type\": \"double\"},{\"name\": \"begin_lon\", \"type\": \"double\"},"
            + "{\"name\": \"end_lat\", \"type\": \"double\"},{\"name\": \"end_lon\", \"type\": \"double\"},"
            + "{\"name\":\"fare\",\"type\": \"double\"}]}";

    public static Schema avroSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
    private static final Random RAND = new Random(46474747);
    private static String tableType = HoodieTableType.MERGE_ON_READ.name();

   public static void main(String[] args) throws Exception {
        try {
            while (true) {
                List<HoodieRecord<HoodieAvroPayload>> records = processHBaseData();
                writeIntoHudi(records);
            }
        } catch (Exception e) {
            log.error("write into hudi error:" + e.getMessage());
        }
   }

    private static void writeIntoHudi(List<HoodieRecord<HoodieAvroPayload>> records) throws Exception {
        String tablePath = "hdfs://bdc-e16-13u-mrs-atlas800-02:25019/user/hive/warehouse/hudi_etl/hudi_wxh.db/test_table";
        String tableName = "test_table";
        Configuration hadoopConf = new Configuration();
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(tableType)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(hadoopConf, tablePath);
        }

        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(avroSchema.toString()).withParallelism(10, 10)
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
            e.printStackTrace();
        } finally {
            hoodieWriteClient.close();
        }
    }

    private static List<HoodieRecord<HoodieAvroPayload>> processHBaseData() {
        List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
        for (String partitionPath : DEFAULT_PARTITION_PATHS) {
            HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
            String commitTime = HoodieActiveTimeline.createNewInstantTime();
            HoodieRecord<HoodieAvroPayload> record = generateUpdateRecord(key, commitTime);
            records.add(record);
        }
        return records;
    }

    public static HoodieRecord<HoodieAvroPayload> generateUpdateRecord(HoodieKey key, String commitTime) {
        return new HoodieAvroRecord<>(key, generateRandomValue(key, commitTime));
    }

    public static HoodieAvroPayload generateRandomValue(HoodieKey key, String commitTime) {
        GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
        return new HoodieAvroPayload(Option.of(rec));
    }

    public static GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName,
                                                      long timestamp) {
        GenericRecord rec = new GenericData.Record(avroSchema);
        rec.put("uuid", rowKey);
        rec.put("ts", timestamp);
        rec.put("rider", riderName);
        rec.put("driver", driverName);
        rec.put("begin_lat", RAND.nextDouble());
        rec.put("begin_lon", RAND.nextDouble());
        rec.put("end_lat", RAND.nextDouble());
        rec.put("end_lon", RAND.nextDouble());
        rec.put("fare", RAND.nextDouble() * 100);
        return rec;
    }
}