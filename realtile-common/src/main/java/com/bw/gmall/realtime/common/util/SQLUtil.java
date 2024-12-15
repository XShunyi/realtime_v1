package com.bw.gmall.realtime.common.util;

import com.bw.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaTopicDb(String groupId) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` Map<String,String>,\n" +
                "   proc_time as proctime(),\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts * 1000,3) ,\n" +
                "  `old` Map<String,String>,\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")"+getKafkaSourceSQL(Constant.TOPIC_DB,groupId);
    }

    // 链接器
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    /**
     * 获取kafka链接
     *
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSinkSQL(String topicName) {
        // key.format  必选
        // value.format 必选
        // https://cloud.tencent.com/developer/article/1806609
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";

    }


    /**
     * 获取kafka链接
     *
     * @param topicName
     * @return
     */
    public static String getKafkaSinkSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
}
