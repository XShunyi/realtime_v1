package com.sy.func;

import com.alibaba.fastjson.JSONObject;
import com.sy.domain.TableProcessDwd;
import com.sy.util.ConfigUtils;
import com.sy.util.JdbcUtils;
import com.sy.util.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DwdProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    private  HashMap<String, TableProcessDwd> hashMap = new HashMap<>();
    private final MapStateDescriptor<String, TableProcessDwd> state;

    private final String querySQL = "select * from gmall2024_config.table_process_dwd";

    public DwdProcessFunction(MapStateDescriptor<String, TableProcessDwd> state) {
        this.state = state;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 读取配置表
        Connection mySQLConnection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd")
        );
        List<TableProcessDwd> tableProcessDwdList = JdbcUtils.queryList(mySQLConnection,
                querySQL,
                TableProcessDwd.class,
                true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            // hash表的String和TableProcessDwd（key,value）
            hashMap.put(tableProcessDwd.getSourceTable(), tableProcessDwd);
//            System.out.println("HashMap已加入");
//        TableProcessDwd(sourceTable=favor_info, sourceType=insert,
//        sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r)
        }
//        for (String s : hashMap.keySet()) {
//            TableProcessDwd value = hashMap.get(s);
//            System.out.println(s+"--"+value);
//        }
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(state);
//        {"op":"r",
//        "after":{"birthday":6127,"gender":"M","create_time":1733924370000,"login_name":"3l9ipnej","nick_name":"阿会","name":"唐会","user_level":"2","phone_num":"13819856181","id":142,"email":"3l9ipnej@live.com"},
//        "source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"user_info"},
//        "ts_ms":1735126811131}
        String table = jsonObject.getJSONObject("source").getString("table");

//        TableProcessDwd = broadcastState.get(table);

        if(table!=null && hashMap.get(table)!=null){
            if(hashMap.get(table).getSourceTable().equals(table)){
                String op = jsonObject.getString("op");
                if(!"d".equals(op)){
                    String sinkTable = hashMap.get(table).getSinkTable();
                    JSONObject jsonObject1 = new JSONObject();
                    jsonObject1.put("op",op);
                    jsonObject1.put("tableName",table);
                    jsonObject1.put("data",jsonObject.getJSONObject("after"));
                    ArrayList<JSONObject> list = new ArrayList<>();
                    list.add(jsonObject1);
                    KafkaUtils.sinkJson2KafkaMessage(sinkTable,list);
                }
//                else{
//                    String sinkTable = hashMap.get(table).getSinkTable();
//                    JSONObject jsonObject1 = new JSONObject();
//                    jsonObject1.put("op",op);
//                    jsonObject1.put("tableName",table);
//                    jsonObject1.put("data",jsonObject.getJSONObject("before"));
//                    ArrayList<JSONObject> list = new ArrayList<>();
//                    list.add(jsonObject1);
//                    KafkaUtils.sinkJson2KafkaMessage(sinkTable,list);
//                }
            }

//        }else{
//            System.out.println("错误数据，不显示");
        }


    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(state);
        String op = tableProcessDwd.getOp();
        // user_info-insert
        String key = tableProcessDwd.getSourceTable();
        try {
            if ("d".equals(op)) {
                broadcastState.remove(key);
                hashMap.remove(key);
            } else {
                broadcastState.put(key, tableProcessDwd);
            }
        } catch (Exception e) {
            System.out.println("错误错误错误！！！");
        }
    }

}