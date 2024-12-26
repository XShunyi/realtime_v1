package com.sy;


import com.alibaba.fastjson.JSONObject;
import com.sy.func.FlinkRMysql;
import com.sy.func.MapUpdateHbaseDimTableFunc;
import com.sy.func.ProcessSpiltStreamToHBaseDim;
import com.sy.util.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadMysql {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//---------------------------------------------------------------------------------------------------------------------------------------------

        // Flink-CDC--read-->Mysql
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


        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = stream_db.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

//        cdcDbMainStreamMap.print("cdcDbMainStreamMap===>");
//{"op":"r",
// "after":{"log":"{\"common\":{\"ar\":\"4\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"is_new\":\"1\",\"md\":\"iPhone 14 Plus\",\"mid\":\"mid_394\",\"os\":\"iOS 12.4.1\",\"sid\":\"f79d567a-18d1-417e-afbb-a8698e845826\",\"uid\":\"40\",\"vc\":\"v2.1.134\"},
// \"page\":{\"during_time\":10212,\"item\":\"17\",\"item_type\":\"sku_ids\",\"last_page_id\":\"good_detail\",\"page_id\":\"order\"},\"ts\":1731282768532}","id":24603},
// "source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"z_log"},
// "ts_ms":1734919118113}

        // Read gmall_config
        MySqlSource<String> stream_config = FlinkRMysql.getcdc2mysql(
                ConfigUtils.getString("mysql.config_database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        DataStreamSource<String> stream_dim = env.fromSource(
                stream_config, WatermarkStrategy.noWatermarks(), "MysqlDimSource"
        );

        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = stream_dim.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);

//        cdcDbDimStreamMap.print("cdcDbDimStreamMap===>");
//{"op":"r",
// "after":{"source_type":"insert","sink_table":"dwd_user_register","source_table":"user_info","sink_columns":"id,create_time"},
// "source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024_config","table":"table_process_dwd"},
// "ts_ms":1734922130547}
//---------------------------------------------------------------------------------------------------------------------------------------------

        SingleOutputStreamOperator<JSONObject> DimCleanColumMap = cdcDbDimStreamMap.map(t -> {

            JSONObject resJson = new JSONObject();
            if ("d".equals(t.getString("op"))) {
                resJson.put("after", t.getJSONObject("before"));
            } else {
                resJson.put("after", t.getJSONObject("after"));
            }
            resJson.put("op", t.getString("op"));
            return resJson;

        }).uid("clean_colum_map")
        .name("clean_colum_map");

        DimCleanColumMap.print("DimCleanColumMap===》");
//{"op":"r",
// "after":{"source_type":"insert","sink_table":"dwd_tool_coupon_get","source_table":"coupon_use","sink_columns":"id,coupon_id,user_id,get_time,coupon_status"}}

        SingleOutputStreamOperator<JSONObject> DS = DimCleanColumMap.map(new MapUpdateHbaseDimTableFunc(ConfigUtils.getString("zookeeper.server.host.list"), ConfigUtils.getString("hbase.namespace")))
                .uid("Create_Hbase_Dim_table")
                .name("Create_Hbase_Dim_table");

        DS.print("DS===>");
//{"op":"r",
// "after":{"source_type":"update","sink_table":"dwd_tool_coupon_use","source_table":"coupon_use","sink_columns":"id,coupon_id,user_id,order_id,using_time,used_time,coupon_status"}}
//        // 状态

        MapStateDescriptor<String, JSONObject> mapState = new MapStateDescriptor<>("mapState", String.class, JSONObject.class);
//        System.out.println("mapState:"+mapState);
//        mapState:MapStateDescriptor{name=mapState, defaultValue=null, serializer=null}
//        // 创建广播流
        BroadcastStream<JSONObject> broadcast = DS.broadcast(mapState);
//        System.out.println("broadcast:"+broadcast);
//        broadcast:org.apache.flink.streaming.api.datastream.BroadcastStream@5276e6b0
//        // 连接
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcast);
//        System.out.println("connectDs:"+connectDs);
//        connectDs:org.apache.flink.streaming.api.datastream.BroadcastConnectedStream@7c469c48
//        // 处理流
        connectDs.process(new ProcessSpiltStreamToHBaseDim(mapState));

        env.execute();
    }
}
