package com.sy.util;



public class SQLUtil {
    private static final String TOPIC_DB="topic_db";

    public static String getKafkaDDLSource(String groupId, String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + ConfigUtils.getString("kafka.bootstrap.servers") + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + ConfigUtils.getString("kafka.bootstrap.servers") + "'," +
                "  'format' = 'json' " +
                ")";
    }

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
                ")"+getKafkaSourceSQL(TOPIC_DB,groupId);

    }

    // 链接器
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + ConfigUtils.getString("kafka.bootstrap.servers") + "',\n" +
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
    public static String getKafkaSinkSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + ConfigUtils.getString("kafka.bootstrap.servers") + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

}