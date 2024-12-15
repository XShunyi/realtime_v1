package com.bw.gmall.realtime.dwd.db.spilt.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.DateFormatUtil;
import com.bw.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new  DwdBaseLog().start(Constant.TOPIC_LOG,Constant.TOPIC_DWD_BASE_LOG,4,10011);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {
//        1.ETL清洗数据
        SingleOutputStreamOperator<JSONObject>  etlStream= etl(dataStreamSource);
//        2.添加水位线keyby
        KeyedStream<JSONObject, String> keyedStream = getKeyedStream(etlStream);
//        3.新老用户校验
        SingleOutputStreamOperator<JSONObject> isNewStream = fixlsNew(keyedStream);
//        isNewStream.print();
//        4.分流
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));
        SingleOutputStreamOperator<String> splitStream = getSplitStream(isNewStream, startTag, errorTag, displayTag, actionTag);
//        5.写入到kafka
        SideOutputDataStream<String> startStream = splitStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = splitStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = splitStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = splitStream.getSideOutput(actionTag);

        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        splitStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    private SingleOutputStreamOperator<String> getSplitStream(SingleOutputStreamOperator<JSONObject> isNewStream, OutputTag<String> startTag, OutputTag<String> errorTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject err=jsonObject.getJSONObject("err");
                if(err!=null){
                    context.output(errorTag,jsonObject.toJSONString());
                    jsonObject.remove("err");
                }

                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                if(start!=null){
                    context.output(startTag,jsonObject.toJSONString());
                    jsonObject.remove("start");
                }else if(page!=null){
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if(displays!=null){
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page",page);
                            display.put("common",common);
                            display.put("ts",ts);
                            context.output(displayTag,display.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if(actions!=null){
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page",page);
                            action.put("common",common);
                            action.put("ts",ts);
                            context.output(actionTag,action.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> fixlsNew(KeyedStream<JSONObject, String> keyedStream) {
        SingleOutputStreamOperator<JSONObject> isNewStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 先取到is_new
                // 0 老用户
                // 1 新用户
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                // 数据日期
                String curDate = DateFormatUtil.tsToDate(ts);
                String isNew = common.getString("is_new");
                // 取出状态的值
                String value = state.value();
                // 如果他是新用户
                if ("1".equals(isNew)) {
                    if (value != null && !curDate.equals(value)) {
                        common.put("is_new", 0);
                    } else {
                        state.update(curDate);
                    }
                }
                return jsonObject;
            }
        });
        return isNewStream;

    }

    private KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> etlStream) {
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }



    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> dataStreamSource) {
        return dataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                if(s!=null){
                  JSONObject jsonObject = JSON.parseObject(s);
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if(common!=null && ts!=null){
                        String mid = common.getString("mid");
                        if(mid!=null && !"".equals(mid)){
                            collector.collect(jsonObject);
                        }
                    }
                }
            }
        });





    }
}
