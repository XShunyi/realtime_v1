package com.sy.func;

import com.sy.util.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class FlinkRMysql {

    public static MySqlSource<String> getcdc2mysql(String database, String table, String username, String pwd, StartupOptions model){

        return MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database) // monitor all tables under inventory database
                .tableList(table)
                .username(username)
                .password(pwd)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                .serverTimeZone(ConfigUtils.getString("mysql.timezone"))
                .startupOptions(model)
                .build();
    }
}
