package dwd.interaction_comment_info;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sy.func.FlinkRKafka;
import com.sy.util.BaseSqlApp;
import com.sy.util.ConfigUtils;
import com.sy.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo {
    // 首先要消费页数主题数据（topic_db）
    // 然后筛选出评论数据并且封装为表
    // 再建立hbase表 字典表
    // 字典表和评论表进行关联 退化成了加购表
    // 然后写入kafka评论事实主题

    private static final String dwd_interaction_comment_info = "dwd_interaction_comment_info";
    private static final String TOPIC_DB = "topic_db";
    private static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    private static final String GROUP_DB = "group_db";
    private static final String group_comment = "group_comment";

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 读取kafka topic_db  数据
        readOdsDb(tableEnv,GROUP_DB);
        getKafkaTopicDb();
        // 过滤评论数据
        Table table = clean_comment(tableEnv);
        // 创建表
        tableEnv.createTemporaryView("comment_info",table);
        // 创建字典表和数据
        createBaseDic(tableEnv);
        // 关联数据
        Table baseDic_Db = getTable(tableEnv);
        // 输出查看
        baseDic_Db.execute().print();
        // 发送到kafka
//        extracted(tableEnv);

        env.execute();
    }

    private static void extracted(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + TOPIC_DWD_INTERACTION_COMMENT_INFO + "(" +
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
                + SQLUtil.getKafkaSinkSQL(TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

//
    private static Table getTable(StreamTableEnvironment tableEnv) {
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

    private static Table clean_comment(StreamTableEnvironment tableEnv) {
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
//
    public static String getKafkaTopicDb() {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` Map<String,String>,\n" +
                "   proc_time as proctime(),\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts * 1000,3) ,\n" +
                "  `old` Map<String,String>,\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")"+FlinkRKafka.getKafkaSource(TOPIC_DB,GROUP_DB,OffsetsInitializer.earliest());
    }

    // 子类调用
    public static void readOdsDb(StreamTableEnvironment tableEnv, String groupId){
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(groupId));
    }

    //读取HBase的base_dic字典表
    public static void createBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '"+ ConfigUtils.getString("hbase.zookeeper_quorum")+"'\n" +
                ")");
    }
}
