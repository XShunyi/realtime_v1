package com.bw.gmall.realtime.dwd.db.app;

import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeRefundPaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS,4,10018);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 读取 topic_db
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        // 2. 读取 字典表
        createBaseDic(tableEnv);

        // 3. 过滤退款成功表数据
        Table refundPayment = tableEnv.sqlQuery(
            "select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "data['total_amount'] total_amount," +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='refund_payment' " +
                "and `type`='update' " +
                "and `old`['refund_status'] is not null " +
                "and `data`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // 4. 过滤退单表中的退单成功的数据
        Table orderRefundInfo = tableEnv.sqlQuery(
            "select " +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['refund_num'] refund_num " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_refund_info' " +
                "and `type`='update' " +
                "and `old`['refund_status'] is not null " +
                "and `data`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 5. 过滤订单表中的退款成功的数据
        Table orderInfo = tableEnv.sqlQuery(
            "select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and `data`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 6. 4 张表的 join
        Table result = tableEnv.sqlQuery(
            "select " +
                "rp.id," +
                "oi.user_id," +
                "rp.order_id," +
                "rp.sku_id," +
                "oi.province_id," +
                "rp.payment_type," +
                "dic.info.dic_name payment_type_name," +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                "rp.callback_time," +
                "ori.refund_num," +
                "rp.total_amount," +
                "rp.ts " +
                "from refund_payment rp " +
                "join order_refund_info ori " +
                "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                "join order_info oi " +
                "on rp.order_id=oi.id " +
                "join base_dic for system_time as of rp.proc_time as dic " +
                "on rp.payment_type=dic.rowkey ");

        // 7.写出到 kafka
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                            "id string," +
                            "user_id string," +
                            "order_id string," +
                            "sku_id string," +
                            "province_id string," +
                            "payment_type_code string," +
                            "payment_type_name string," +
                            "date_id string," +
                            "callback_time string," +
                            "refund_num string," +
                            "refund_amount string," +
                            "ts bigint " +
                            ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
}
