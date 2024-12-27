package com.sy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sy.domain.TableProcessDwd;
import com.sy.func.DwdProcessFunction;

import com.sy.func.FlinkRKafka;
import com.sy.func.FlinkRMysql;
import com.sy.util.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



public class ReadDb {

    private static final String topic_db = "topic_db";
    private static final String topic_group = "topic_db";
    private static final String config_database = "mysql.config_database";
    private static final String user = "mysql.user";
    private static final String pwd = "mysql.pwd";



    @SneakyThrows
    public static void main(String[] args) {

        // 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取db数据
        KafkaSource<String> kafkaSource = FlinkRKafka.getKafkaSource(
                topic_db,
                topic_group,
                OffsetsInitializer.earliest()
        );
        DataStreamSource<String> stream_db = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
//        stream_db.print("stream_db===>");



        // 进行数据清洗
        SingleOutputStreamOperator<JSONObject> clean_stream =stream_db.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).uid("clean_db").name("clean_db");
//        clean_stream.print("clean_stream===>");

        // 读取配置表
        MySqlSource<String> mySqlSource = FlinkRMysql.getcdc2mysql(
                ConfigUtils.getString(config_database),
                "",
                ConfigUtils.getString(user),
                ConfigUtils.getString(pwd),
                StartupOptions.initial()
        );
        DataStreamSource<String> stream_config = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource");
//        stream_config.print("stream_config===>");


        // 过滤数据
        SingleOutputStreamOperator<TableProcessDwd> clean_config;
        clean_config = stream_config.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) {
                JSONObject jsonObject = JSON.parseObject(s);
                String op = jsonObject.getString("op");
                TableProcessDwd TableProcessDwd;
                if ("d".equals(op)) {
                    TableProcessDwd = JSON.parseObject(jsonObject.getString("before"), TableProcessDwd.class);
                } else {
                    TableProcessDwd = JSON.parseObject(jsonObject.getString("after"), TableProcessDwd.class);
                }
                TableProcessDwd.setOp(op);
                collector.collect(TableProcessDwd);
            }
        }).uid("clean_config").name("clean_config");
//        clean_config.print("clean_config===>");

        // 创建状态
        MapStateDescriptor<String, TableProcessDwd> state = new MapStateDescriptor<>("state", String.class, TableProcessDwd.class);
        // 创建广播流
        BroadcastStream<TableProcessDwd> broadcast = clean_config.broadcast(state);
        // 主流和配置表连接
        BroadcastConnectedStream<JSONObject, TableProcessDwd> db_connect_config = clean_stream.connect(broadcast);
        // 处理流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> process = db_connect_config.process(new DwdProcessFunction(state))
                .uid("process_broadcast")
                .name("process_broadcast");
        process.print("process===>");

        // 转换格式类型
//        SingleOutputStreamOperator<String> data;
//        data = process.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, String>() {
//            @Override
//            public String map(Tuple2<JSONObject, TableProcessDwd> s) {
//                String toString = s.f0.toString();
//                return JSON.parseObject(toString).toJSONString();
//            }
//        }).uid("to_jsonString").name("to_jsonString");
//        data.print("data===>");

        // 发送到kafka
//        data.sinkTo(Flink2Kafka.getSinkKafka(topic_dwd_db));

        env.execute();
    }

}
