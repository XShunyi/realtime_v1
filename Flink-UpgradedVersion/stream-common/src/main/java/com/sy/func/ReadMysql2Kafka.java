package com.sy.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sy.util.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadMysql2Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取gmall数据库
        MySqlSource<String> stream = FlinkRMysql.getcdc2mysql(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
// {"before":null,
// "after":{"id":348,"user_id":1163,"nick_name":"琦琦","head_img":null,"sku_id":27,"spu_id":9,"order_id":4133,"appraise":"1201","comment_txt":"评论内容：67965377968966389525767695775612659235747698537276","create_time":1731324117000,"operate_time":null},
// "source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall","sequence":null,"table":"comment_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
// "op":"r",
// "ts_ms":1735288233085,
// "transaction":null}
        DataStreamSource<String> stream_db = env.fromSource(
                stream, WatermarkStrategy.noWatermarks(), "MysqlMainSource"
        ).setParallelism(1);
//        stream_db.print();


        // 发送到kafka
        stream_db.sinkTo(Flink2Kafka.getSinkKafka("topic_db"))
                .uid("mysql_to_topic_db")
                .name("mysql_to_topic_db");

        env.execute();





    }
}
