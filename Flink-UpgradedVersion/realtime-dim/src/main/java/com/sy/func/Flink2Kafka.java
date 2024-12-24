package com.sy.func;

import com.alibaba.fastjson.JSONObject;
import com.sy.util.ConfigUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class Flink2Kafka {
    public static KafkaSource<String> getKafkaSource(String topic,String groupId,OffsetsInitializer model){

        return KafkaSource.<String>builder()
                .setBootstrapServers(ConfigUtils.getString("kafka.bootstrap.servers"))
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(model)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
