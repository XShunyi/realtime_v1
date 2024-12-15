package com.bw.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.bean.TableProcessDim;

import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.HbaseUtil;
import com.bw.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {


//    （1）定义成员变量jedis
//    private Jedis jedis;

    private Connection hbaseConnect;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConnect = HbaseUtil.getHbaseConnect();
//        jedis = RedisUtil.getJedis();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {

        // 主流维度数据
        JSONObject f0 = value.f0;
        // 对应的配置表数据
        TableProcessDim f1 = value.f1;
        System.out.println("正在写入维度数据"+f1.getSourceTable());
        // 要往Hbase写的数据
        JSONObject data = f0.getJSONObject("data");
        // maxwell
        String type = f0.getString("type");
        // 要往Hbase写的哪张表
        String sinkTable = f1.getSinkTable();
        String rowKey = f1.getSinkRowKey();// "id"
        String sinkFamily = f1.getSinkFamily();
        // 拿到rowkey具体的值
        String rowKeyValue = data.getString(rowKey);
        String redisKey = RedisUtil.getKey(sinkTable, rowKeyValue);
        if ("delete".equals(type)){
            HbaseUtil.deleteCells(hbaseConnect, Constant.HBASE_NAMESPACE,sinkTable,rowKeyValue);
//            jedis.del(redisKey);
        }else{
            HbaseUtil.putCells(hbaseConnect, Constant.HBASE_NAMESPACE,sinkTable,rowKeyValue,sinkFamily,data);
//             jedis.setex(redisKey,24*3600,data.toJSONString());
        }
//在invoke方法中补充缓存清除操作


    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHBaseConn(hbaseConnect);
//        RedisUtil.closeJedis(jedis);
    }




}
