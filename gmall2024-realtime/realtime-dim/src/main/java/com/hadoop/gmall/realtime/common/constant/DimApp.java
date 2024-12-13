package com.hadoop.gmall.realtime.common.constant;





import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.hadoop.gmall.realtime.common.bean.TableProcessDim;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO: 基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        // TODO: 检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        // 是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        // 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//        // 设置状态后端  检查路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("http://hadoop102:8020/ck");
//        // 设置hadoop用户
        System.setProperty("HADOOP_USER_NAME","hadoop102");

//        // TODO: 消费kafka数据
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers(Constant.KAFKA_BROKERS)
//                .setTopics(Constant.TOPIC_DB)
//                .setGroupId("dim_app_group")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(
//                        new DeserializationSchema<String>() {
//                            @Override
//                            public String deserialize(byte[] message) throws IOException {
//                                if(message!=null){
//                                    return new String(message);
//                                }
//                                return null;
//                            }
//
//                            @Override
//                            public boolean isEndOfStream(String s) {
//                                return false;
//                            }
//
//                            @Override
//                            public TypeInformation<String> getProducedType() {
//                                return TypeInformation.of(String.class);
//                            }
//                        }
//                )
//                .build();
//
//
//        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//
//        SingleOutputStreamOperator<JSONObject> jsonobjectDS = kafkaStrDS.process(
//                new ProcessFunction<String, JSONObject>() {
//                    @Override
//                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                        JSONObject jsonObject = JSON.parseObject(s);
//                        String db = jsonObject.getString("database");
//                        String type = jsonObject.getString("type");
//                        String data = jsonObject.getString("data");
//                        if ("gmall".equals(db)
//                                && ("insert".equals(type)
//                                || "update".equals(type)
//                                || "delete".equals(type)
//                                || "bootstrap-insert".equals(type)
//                                && data != null
//                                && data.length() > 2)) {
//
//                        }
//                        collector.collect(jsonObject);
//                    }
//                }
//        );
//        jsonobjectDS.print();


        // 读取配置表
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(3306)
                .databaseList("gmall2023_config") // monitor all tables under inventory database
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationUtil()) // converts SourceRecord to String
                .startupOptions(StartupOptions.initial())
                .build();



        DataStreamSource<String> streamSource = env.addSource(sourceFunction).setParallelism(1);
//        streamSource.print();












        env.execute();
    }
//    private SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
//        // useSSL=false
//        Properties props = new Properties();
//        props.setProperty("useSSL", "false");
//        props.setProperty("allowPublicKeyRetrieval", "true");
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname(Constant.MYSQL_HOST)
//                .port(Constant.MYSQL_PORT)
//                .databaseList("gmall2023_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
//                .tableList("gmall2023_config.table_process_dim") // set captured table
//                .username(Constant.MYSQL_USER_NAME)
//                .password(Constant.MYSQL_PASSWORD)
//                .jdbcProperties(props)
//                .deserializer(new JsonDebeziumDeserializationUtil()) // converts SourceRecord to JSON String
//                .startupOptions(StartupOptions.initial()) // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
//                .build();
//
//        return env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
//                .setParallelism(1) // 并行度设置为 1
//                .map(new MapFunction<Object, TableProcessDim>() {
//                    @Override
//                    public TableProcessDim map(Object o) throws Exception {
//                        return null;
//                    }
//                })
//                .map(new MapFunction<String, TableProcessDim>() {
//                    @Override
//                    public TableProcessDim map(String value) throws Exception {
//                        JSONObject obj = JSON.parseObject(value);
//                        String op = obj.getString("op");
//                        TableProcessDim tableProcessDim;
//                        if ("d".equals(op)) {
//                            tableProcessDim = obj.getObject("before", TableProcessDim.class);
//                        } else {
//                            tableProcessDim = obj.getObject("after", TableProcessDim.class);
//                        }
//                        tableProcessDim.setOp(op);
//
//                        return tableProcessDim;
//                    }
//                })
//                .setParallelism(1);
}

//    }
