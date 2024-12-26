package com.sy.func;

import com.sy.util.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

        DataStreamSource<String> stream_db = env.fromSource(
                stream, WatermarkStrategy.noWatermarks(), "MysqlMainSource"
        );

        // 发送到kafka
        stream_db.sinkTo(Flink2Kafka.getSinkKafka("topic_db"))
                .uid("mysql_to_topic_db")
                .name("mysql_to_topic_db");

        env.execute();





    }
}
