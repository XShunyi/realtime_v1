package com.bw.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {
    private final MapStateDescriptor<String,TableProcessDim> mapDescriptor;

    public DimProcessFunction(MapStateDescriptor<String,TableProcessDim> mapDescriptor) {
        this.mapDescriptor = mapDescriptor;
    }

    private HashMap<String,TableProcessDim> hashMap = new HashMap<>();
    @Override
    public void open(Configuration parameters) throws Exception {
        // 手动读取配置表
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 主流逻辑
        String table = jsonObject.getString("table");
        // 读取状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapDescriptor);
        // 根据table取值
        TableProcessDim tableProcessDim = broadcastState.get(table);

        if (tableProcessDim == null){
            tableProcessDim= hashMap.get(table);
        }

        if (tableProcessDim != null){
            System.out.println("jsonObject = " + jsonObject);
            System.out.println("tableProcessDim = " + tableProcessDim);
            collector.collect(Tuple2.of(jsonObject,tableProcessDim));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 广播流逻辑
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapDescriptor);
        broadcastState.put(tableProcessDim.getSourceTable(),tableProcessDim);
        // 如果配置表删除了，要从状态移出去数据
        String op = tableProcessDim.getOp();
        if ("d".equals(op)){
            broadcastState.remove(tableProcessDim.getSourceTable());
            hashMap.remove(tableProcessDim.getSourceTable());
        }
    }
}
