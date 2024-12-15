package com.bw.gmall.realtime.dwd.db.app;

import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {

        new DwdTradeCartAdd().start(Constant.TOPIC_DWD_TRADE_CART_ADD,4,10013);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
//        1.读取ods层数据
        readOdsDb(tableEnv, groupId);
//        2.过滤数据
        Table table = getTable(tableEnv);
//        3.在kafka的链接表
        getTableResult(tableEnv);
//        4.写入kafka
        table.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();

    }

    private void getTableResult(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                " id  STRING,\n" +
                " user_id STRING,\n" +
                " sku_id STRING,\n" +
                " cart_price STRING,\n" +
                " sku_num BIGINT,\n" +
                " sku_name STRING,\n" +
                " is_checked STRING,\n" +
                " create_time STRING,\n" +
                " operate_time STRING,\n" +
                " is_ordered STRING,\n" +
                " order_time STRING,\n" +
                " source_type STRING,\n" +
                " source_id STRING,\n" +
                "   ts BIGINT)" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    private Table getTable(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select \n" +
                " `data`['id'] id, \n" +
                " `data`['user_id'] user_id, \n" +
                " `data`['sku_id'] sku_id, \n" +
                " `data`['cart_price'] cart_price, \n" +
                "  if(`type`='insert',cast(`data`['sku_num'] as bigint),cast(`data`['sku_num'] as bigint)-cast(`old`['sku_num'] as bigint)) sku_num, \n" +
                " `data`['sku_name'] sku_name, \n" +
                " `data`['is_checked'] is_checked, \n" +
                " `data`['create_time'] create_time, \n" +
                " `data`['operate_time'] operate_time, \n" +
                " `data`['is_ordered'] is_ordered, \n" +
                " `data`['order_time'] order_time,\n" +
                " `data`['source_type'] source_type, \n" +
                " `data`['source_id'] source_id,\n" +
                "  ts\n" +
                "from topic_db\n" +
                "where `database` = 'gmall' and `table` = 'cart_info'\n" +
                "and (`type` = 'insert' or (`type` = 'update' and `old`['sku_num'] is not null\n" +
                "and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");
        tableEnv.createTemporaryView("cart_info", table);
        return table;
    }
}
