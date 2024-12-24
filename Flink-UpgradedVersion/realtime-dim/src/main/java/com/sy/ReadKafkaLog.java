package com.sy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sy.func.Flink2Kafka;
import com.sy.func.FlinkSendKafka;
import com.sy.util.DateFormatUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.Duration;

public class ReadKafkaLog {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: Read Topic_log
        KafkaSource<String> kafkaSource = Flink2Kafka.getKafkaSource(
                "topic_log",
                "test",
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource").setParallelism(1);

        //        stream.print("source===>");

        // TODO: 新老用户校验

        SingleOutputStreamOperator<JSONObject> mapDs = stream.keyBy(x->JSON.parseObject(x).getJSONObject("common")).map(new RichMapFunction<String, JSONObject>() {
            ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new", String.class));
            }

            @Override
            public JSONObject map(String s) throws Exception {
                // TODO: 先拿到字段 "is_new"
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject common = jsonObject.getJSONObject("common");
                String is_new = common.getString("is_new");
                // TODO: 数据日期
                Long ts = jsonObject.getLong("ts");
                String toDate = DateFormatUtil.tsToDate(ts);
                // TODO: 取状态的值
                String value = state.value();
                // TODO: 判断
                if ("1".equals(is_new)) {
                    if (value != null && !toDate.equals("")) {
                        common.put("is_new", 0);
                    } else {
                        state.update(toDate);
                    }
                }
                return jsonObject;
            }
            // TODO: 设置水平线
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event,timestamp)->event.getLong("ts")));


        // TODO: 将数据进行分流 actions  common(主流)  displays page start  err
        OutputTag<String> actionsTag = new OutputTag<String>("actions-output") {};
        OutputTag<String> displaysTag = new OutputTag<String>("displays-output") {};
        OutputTag<String> pageTag = new OutputTag<String>("page-output") {};
        OutputTag<String> startTag = new OutputTag<String>("start-output") {};
        OutputTag<String> errTag = new OutputTag<String>("err-output") {};

        // TODO: 分流
        SingleOutputStreamOperator<String> process = mapDs.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // TODO: 过滤
                if (jsonObject != null) {
                    JSONObject common = jsonObject.getJSONObject("common");
                    String mid = common.getString("mid");
                    if (common != null && mid != null) {
                        // TODO: 获取各测流
                        String start = jsonObject.getString("start");
                        String actions = jsonObject.getString("actions");
                        String displays = jsonObject.getString("displays");
                        String page = jsonObject.getString("page");
                        String err = jsonObject.getString("err");
                            // TODO: err
                        if (err != null) {
                                context.output(errTag, jsonObject.toString());
                            // TODO: start
                        }else if (start != null) {
                                context.output(startTag, jsonObject.toString());
                        }else if (page != null) {
                            // TODO: displays
                            if (displays != null) {
                                context.output(displaysTag, jsonObject.toString());
                            } else {
                            // TODO: page
                                context.output(pageTag, jsonObject.toString());
                            }
                        }else if (actions != null) {
                            // TODO: actions
                                context.output(actionsTag, jsonObject.toString());
                        }
                        // TODO: common
                        collector.collect(jsonObject.toString());
                    }
                }
            }
        });
        // TODO: 输出
        // 主流
        process.print("common===>");
        // 测流
        process.getSideOutput(actionsTag).print("actions===>");
        process.getSideOutput(displaysTag).print("displays===>");
        process.getSideOutput(pageTag).print("page===>");
        process.getSideOutput(startTag).print("start===>");
        process.getSideOutput(errTag).print("err===>");

        // TODO: 发送数据到kafka
        // 主流
//        process.sinkTo(FlinkSendKafka.getSinkKafka("log_common")).uid("F2K_common").name("F2K_common");
        // 测流
//        process.getSideOutput(actionsTag).sinkTo(FlinkSendKafka.getSinkKafka("log_actions")).uid("F2K_actions").name("F2K_actions");
//        process.getSideOutput(displaysTag).sinkTo(FlinkSendKafka.getSinkKafka("log_displays")).uid("F2K_displays").name("F2K_displays");
//        process.getSideOutput(pageTag).sinkTo(FlinkSendKafka.getSinkKafka("log_page")).uid("F2K_page").name("F2K_page");
//        process.getSideOutput(startTag).sinkTo(FlinkSendKafka.getSinkKafka("log_start")).uid("F2K_start").name("F2K_start");
//        process.getSideOutput(errTag).sinkTo(FlinkSendKafka.getSinkKafka("log_err")).uid("F2K_err").name("F2K_err");








        env.execute();
    }
}
