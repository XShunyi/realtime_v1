package com.bw.gmall.realtime.dwd.db.app;

import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, 4, 10014);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        // 1.读取ODS数据
        readOdsDb(tableEnv, groupId);
        // 2. 过滤订单详情流数据
        filterOrderDetailInfo(tableEnv);
        // 3. 过滤订单数据
        filterOrderInfo(tableEnv);
        // 4. 过滤活动流
        filterOrderActivity(tableEnv);
        // 5. 过滤优惠券流
        filterOrderCoupon(tableEnv);
        // 6. 关联
        Table table = OrderJoin(tableEnv);
        // 7. 写入Kafka
        createKafkaSinkTable(tableEnv);
        table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }


    private void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
        // 注意：如果有left jojn或 right join 或者group by 必须要用upsert kafka
        // 如果要用upsert kafka必须要要有主键
        // 如果要用upsert kafka必须要要有主键
        // 如果要用upsert kafka必须要要有主键
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (\n" +
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
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }


    private Table OrderJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
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
                "  create_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts \n" +
                "from order_detail_info od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id = od.id ");
    }

    private void filterOrderCoupon(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
    }


    private void filterOrderActivity(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['activity_id'] activity_id, \n" +
                "  `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity", odaTable);

    }

    private void filterOrderInfo(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['user_id'] user_id, \n" +
                "  `data`['province_id'] province_id \n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_info", oiTable);
    }

    private void filterOrderDetailInfo(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['order_id'] order_id, \n" +
                "  `data`['sku_id'] sku_id, \n" +
                "  `data`['sku_name'] sku_name, \n" +
                "  `data`['order_price'] order_price, \n" +
                "  `data`['sku_num'] sku_num, \n" +
                "  `data`['create_time'] create_time, \n" +
                "  `data`['split_total_amount'] split_total_amount, \n" +
                "  `data`['split_activity_amount'] split_activity_amount, \n" +
                "  `data`['split_coupon_amount'] split_coupon_amount, \n" +
                "  ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_info", odTable);
    }
}

