package com.sy.util;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSourceUtil {

    /**
     * @param topic   主题
     * @param groupId 消费者组ID
     * @return
     */
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(ConfigUtils.getString("Kafka.bootstrap.servers"))
                .setTopics(topic)
                .setGroupId(groupId)
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
                            return new String(bytes, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }

    /**
     * 读取CDC
     *
     * @param processDataBase
     * @param processDimTableName
     * @return
     */
    public static MySqlSource<String> getMysqlSource(String processDataBase, String processDimTableName) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        return MySqlSource.<String>builder()
                .jdbcProperties(props)
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(ConfigUtils.getString("mysql.database")) // monitor all tables under inventory database
                .username(ConfigUtils.getString("mysql.user"))
                .password(ConfigUtils.getString("mysql.pwd"))
                .tableList(processDataBase + "." + processDimTableName)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                .startupOptions(StartupOptions.initial())
                .build();

    }
}
