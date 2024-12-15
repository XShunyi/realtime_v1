package com.bw.gmall.realtime.dwd.db.app;

import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO,4,10012);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
//        1.读取ods层数据
        readOdsDb(tableEnv,groupId);
//        2.过滤评论数据
        Table comment=getComment(tableEnv);
        tableEnv.createTemporaryView("comment_info",comment);
//        3.字典数据
        //        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
//                "  dic_code STRING,\n" +
//                "  dic_name STRING,\n" +
//                "  parent_code STRING,\n" +
//                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall?useSSL=false',\n" +
//                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
//                "   'table-name' = 'base_dic',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = '123456'\n" +
//                ")");
//        4.读取码表
        createBaseDic(tableEnv);
//        5.关联数据
        Table table = getTable(tableEnv);
//        6.写入到kafka
        extracted(tableEnv);


//        写出去
        table.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }

    private void extracted(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "(" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  nick_name STRING,\n" +
                "  sku_id STRING,\n" +
                "  spu_id STRING,\n" +
                "  order_id STRING,\n" +
                "  appraise_code STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  create_time STRING" +
                ")"
                + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

    private Table getTable(StreamTableEnvironment tableEnv) {
        return  tableEnv.sqlQuery("SELECT \n" +
                " id,\n" +
                " user_id,\n" +
                " nick_name,\n" +
                " sku_id,\n" +
                " spu_id,\n" +
                " order_id,\n" +
                " appraise,\n" +
                " info.dic_name,\n" +
                " comment_txt,\n" +
                " create_time\n" +
                "FROM comment_info AS c\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS b\n" +
                "  ON c.appraise = b.rowkey");
    }

    private Table getComment(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['nick_name'] nick_name,\n" +
                "`data`['sku_id'] sku_id,\n" +
                "`data`['spu_id'] spu_id,\n" +
                "`data`['order_id'] order_id,\n" +
                "`data`['appraise'] appraise,\n" +
                "`data`['comment_txt'] comment_txt,\n" +
                "`data`['create_time'] create_time,\n" +
                " proc_time \n" +
                "from topic_db\n" +
                "where `database` = 'gmall' and `type` = 'insert'\n" +
                "and `table` = 'comment_info'");
    }
}
