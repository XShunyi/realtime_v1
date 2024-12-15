package com.bw.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.bean.TableProcessDwd;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.function.DwdProcessFunction;
import com.bw.gmall.realtime.common.util.FlinkSinkUtil;
import com.bw.gmall.realtime.common.util.FlinkSourceUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Set;

public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(Constant.TOPIC_DB,Constant.TOPIC_DB,4,10019);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {
        // 1.ETL
        SingleOutputStreamOperator<JSONObject> etlStream = etl(dataStreamSource);
        // 2.通过CDC读取配置表,并行度只能是1
        DataStreamSource<String> processStream = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "cdc_stream").setParallelism(1);
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = getTableProcessDwdSingleOutputStreamOperator(processStream);
        processStream.print();
        MapStateDescriptor<String, TableProcessDwd> mapDescriptor = new MapStateDescriptor<String,TableProcessDwd>("broadcast_state",String.class,TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(mapDescriptor);
        // 3.主流跟配置进行连接，根据配置过滤要的流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processBroadStream = etlStream.connect(broadcastStream).process(new DwdProcessFunction(mapDescriptor));
        // 4.过滤字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> filterStream = getFilterStream(processBroadStream);
        // 5.写入Kafka
//        filterStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, String>() {
//            @Override
//            public String map(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2) throws Exception {
//                return jsonObjectTableProcessDwdTuple2.toString();
//            }
//        }).sinkTo(FlinkSinkUtil.getKafkaSink("dsajgj"));
        filterStream.sinkTo(FlinkSinkUtil.getDwdKafkaSink());

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> getFilterStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processBroadStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> filterStream = processBroadStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2) throws Exception {
                JSONObject f0 = jsonObjectTableProcessDwdTuple2.f0;
                TableProcessDwd f1 = jsonObjectTableProcessDwdTuple2.f1;
                JSONObject data = f0.getJSONObject("data");
                String sinkColumns = f1.getSinkColumns();
                Set<String> keys = data.keySet();
                keys.removeIf(key -> !sinkColumns.contains(key));
                return jsonObjectTableProcessDwdTuple2;
            }
        });
        return filterStream;
    }

    private SingleOutputStreamOperator<TableProcessDwd> getTableProcessDwdSingleOutputStreamOperator(DataStreamSource<String> processStream) {
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = processStream.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                TableProcessDwd tableProcessDwd;
                String op = jsonObject.getString("op");
                if ("d".equals(op)) {
                    tableProcessDwd = JSON.parseObject(jsonObject.getString("before"), TableProcessDwd.class);
                } else {
                    tableProcessDwd = JSON.parseObject(jsonObject.getString("after"), TableProcessDwd.class);
                }
                tableProcessDwd.setOp(op);
                collector.collect(tableProcessDwd);
            }
        });
        return processDwdStream;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> dataStreamSource) {
        SingleOutputStreamOperator<JSONObject> etlStream = dataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return etlStream;
    }
}

