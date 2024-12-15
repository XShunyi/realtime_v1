package com.bw.gmall.realtime.dwd.db.app;

import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS, 4, 10015);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        // 1.消费kafka主题dwd_trade_order_detail 订单表
        extracted(tableEnv, groupId);

        // 2.读取字典表数据
        createBaseDic(tableEnv);
        // 3.读取ODS数据
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        // 4.过滤支付成功数据
        Table table = getTable(tableEnv);
        tableEnv.createTemporaryView("payment", table);
        // 5.订单表和支付成功做interval join  关联后数据
        Table joinTable = getJoinTable(tableEnv);
        tableEnv.createTemporaryView("pay_order", joinTable);
        // 6、关联后数据做Look Join
        Table result = getResult(tableEnv);
        // 7、写入Kafka
        extracted(tableEnv);
        result.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();
//        result.execute().print();
    }

    private void extracted(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  payment_type_code  STRING,\n" +
                "  payment_type_name  STRING,\n" +
                "  payment_time  STRING,\n" +
                "  ts  BIGINT\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    private Table getResult(StreamTableEnvironment tableEnv) {
        Table result = tableEnv.sqlQuery("SELECT\n" +
                "  id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  payment_type payment_type_code,\n" +
                "  info.dic_name payment_type_name,\n" +
                "  payment_time,\n" +
                "  ts\n" +
                "FROM pay_order AS p\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF p.proc_time AS b\n" +
                "ON p.payment_type = b.rowkey");
        return result;
    }

    private Table getJoinTable(StreamTableEnvironment tableEnv) {
        Table joinTable = tableEnv.sqlQuery("SELECT \n" +
                "  o.id,\n" +
                "  o.order_id,\n" +
                "  sku_id,\n" +
                "  p.user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  p.ts,\n" +
                "  payment_type,\n" +
                "  callback_time payment_time,\n" +
                "  proc_time     \n" +
                "FROM payment p, order_detail o\n" +
                "WHERE p.order_id = o.order_id\n" +
                "AND p.row_time BETWEEN o.row_time - INTERVAL '15' MINUTE AND p.row_time + INTERVAL '5' SECOND");
        return joinTable;
    }

    private Table getTable(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['callback_time'] callback_time,\n" +
                "     ts,\n" +
                "     row_time,\n" +
                "     proc_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'payment_info'\n" +
                "and `type` = 'update'\n" +
                "and `old`['payment_status'] is not null\n" +
                "and `data`['payment_status'] = '1602'");
        return table;
    }

    private void extracted(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table order_detail(\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  create_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint,\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts * 1000,3) ,\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }
}

