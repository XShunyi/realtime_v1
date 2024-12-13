package com.hadoop.gmall.realtime.dim.test;




import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.hadoop.gmall.realtime.common.constant.JsonDebeziumDeserializationUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class flink_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
//        env.enableCheckpointing(3000);

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2023_config") // monitor all tables under inventory database
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationUtil()) // converts SourceRecord to String
                .build();


        env.addSource(sourceFunction).print(); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
