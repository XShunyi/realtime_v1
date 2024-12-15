package com.bw.gmall.realtime.common.app;

import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.function.DimProcessFunction;
import com.bw.gmall.realtime.common.function.DimSinkFunction;

import lombok.extern.slf4j.Slf4j;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.bean.TableProcessDim;

import com.bw.gmall.realtime.common.util.FlinkSourceUtil;
import com.bw.gmall.realtime.common.util.HbaseUtil;
import com.bw.gmall.realtime.common.util.JdbcUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(Constant.TOPIC_DB, Constant.DIM_APP, 4, 10001);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {
        //  1.ETL清洗主流数据
        SingleOutputStreamOperator<JSONObject> etlStream = etl(dataStreamSource);
        // 2.通过CDC读取配置表,并行度只能是1
        DataStreamSource<String> processStream = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME), WatermarkStrategy.noWatermarks(), "cdc_stream").setParallelism(1);
        // 3.在Hbase建表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createTable(processStream);
        // 4.主流数据和广播进行连接处理
        MapStateDescriptor<String,TableProcessDim> mapDescriptor = new MapStateDescriptor<String,TableProcessDim>("broadcast_state",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = createTableStream.broadcast(mapDescriptor);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processBroadCastStream = etlStream.connect(broadcastStream).process(new DimProcessFunction(mapDescriptor));
        // 5.过滤字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterStream = getFilterStream(processBroadCastStream);
        // 6.写入Hbase
        filterStream.addSink(new DimSinkFunction());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> getFilterStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processBroadCastStream) {
        return processBroadCastStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> processDimTuple2) throws Exception {
                // 主流维度数据
                JSONObject f0 = processDimTuple2.f0;
                // 主流维度对于配置表
                TableProcessDim f1 = processDimTuple2.f1;
                // 要写入的列
                List<String> columns = Arrays.asList(f1.getSinkColumns().split(","));
                JSONObject data = f0.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return processDimTuple2;
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> dataStreamSource) {
        return dataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    if (s != null) {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String db = jsonObject.getString("database");
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
//                        if ("gmall".equals(db)
//                            && "insert".equals(type)
//                            || "update".equals(type)
//                            || "delete".equals(type)
//                            || "bootstrap-insert".equals(type) && data != null && data.length() > 2) {
//                        collector.collect(jsonObject);
//                    }
                        if ("gmall".equals(db) && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type) && data != null && data.length() > 0){
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public SingleOutputStreamOperator<TableProcessDim> createTable( DataStreamSource<String> processStream){
        return processStream.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {

            private Connection hbaseConnect;
            private TableProcessDim tableProcessDim;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 连接Hbase -->ctrl+alt+f
                hbaseConnect = HbaseUtil.getHbaseConnect();
            }

            @Override
            public void close() throws Exception {
                // 关闭连接
                hbaseConnect.close();
            }

            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                // 处理逻辑
                JSONObject jsonObject = JSON.parseObject(s);
                String op = jsonObject.getString("op");
                System.out.println(op);
                if ("r".equals(op) || "c".equals(op)) {
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                    String[] split = tableProcessDim.getSinkFamily().split(",");
                    // 创建表
                    createTable(split);
                } else if ("d".equals(op)) {
                    tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                    // 删除表
                    deleteTable();
                } else if ("u".equals(op)) {
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                    String[] split = tableProcessDim.getSinkFamily().split(",");
                    // 先删除后建
                    deleteTable();
                    createTable(split);
                }
                tableProcessDim.setOp(op);
                collector.collect(tableProcessDim);
            }

            public void createTable(String[] families) {
                try {
                    System.out.println("tableProcessDim.getSinkTable() = " + tableProcessDim.getSinkTable());
                    HbaseUtil.createHBaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), families);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            public void deleteTable() {
                System.out.println("tableProcessDim.getSinkTable() = " + tableProcessDim.getSinkTable());
                try {
                    HbaseUtil.dropHBaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).setParallelism(1);
    }
}

