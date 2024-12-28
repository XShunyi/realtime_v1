package com.sy.func;

import com.alibaba.fastjson.JSONObject;

import com.sy.util.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;



/**
 * @Package com.stream.func.MapUpdateHbaseDimTable
 * @Author zhou.han
 * @Date 2024/12/19 14:11
 * @description:
 */
public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject,JSONObject> {

    private Connection connection;
    private final String hbaseNameSpace;
    private final String zkHostList;
    private HbaseUtils hbaseUtils;

    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils(zkHostList);
        connection = hbaseUtils.getConnection();
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String op = jsonObject.getString("op");
        if ("d".equals(op)){
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
            System.out.println("删除成功");
        }else if ("r".equals(op) || "c".equals(op)){
//            String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
//            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
//            System.out.println("tableName:"+tableName);

            if(hbaseUtils.tableIsExists("gmall:"+tableName)){
//                System.out.println("表gmall:"+tableName+"已存在");
            }else{
                hbaseUtils.createTable(hbaseNameSpace,tableName);
//                hbaseUtils.deleteTable(jsonObject.getJSONObject("after").getString("sink_table"));
            }

        }else {
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
            String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
//            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
//            System.out.println("tableName:"+tableName);
            if(hbaseUtils.tableIsExists("gmall:"+tableName)){
//                System.out.println("表"+tableName+"已存在");
            }else{
                hbaseUtils.createTable(hbaseNameSpace,tableName,columnName);
//                hbaseUtils.deleteTable(jsonObject.getJSONObject("after").getString("sink_table"));
            }
        }
        return jsonObject;
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
