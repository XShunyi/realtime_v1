package com.sy.func;

import com.alibaba.fastjson.JSONObject;
import com.sy.util.ConfigUtils;
import com.sy.util.HbaseUtils;
import com.sy.util.JdbcUtils;
import com.sy.domain.TableProcessDim;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



/**
 * @Package com.retailersv1.func.ProcessSpiltStreamToHBaseDim
 * @Author zhou.han
 * @Date 2024/12/19 22:55
 * @description:
 */
public class ProcessSpiltStreamToHBaseDim extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();
    private final String querySQL = "select * from gmall2024_config.table_process_dim";

    private HbaseUtils hbaseUtils;

    private org.apache.hadoop.hbase.client.Connection hbaseConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
//        System.out.println("tableProcessDims:"+tableProcessDims);
        for (TableProcessDim tableProcessDim : tableProcessDims ){
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);

        }
        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection=hbaseUtils.getConnection();
        connection.close();
    }

    public ProcessSpiltStreamToHBaseDim(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
//        System.out.println("pe:"+jsonObject);
//{"op":"r",
// "after":{"log":"{\"actions\":[{\"action_id\":\"favor_add\",\"item\":\"16\",\"item_type\":\"sku_id\",\"ts\":1731297894669}],\"common\":{\"ar\":\"33\",\"ba\":\"realme\",\"ch\":\"360\",\"is_new\":\"1\",\"md\":\"realme Neo2\",\"mid\":\"mid_413\",\"os\":\"Android 13.0\",\"sid\":\"fef393df-41f7-4321-a8bc-bf6fbe0dd6d4\",\"uid\":\"960\",\"vc\":\"v2.1.134\"},\"displays\":[{\"item\":\"35\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":0},{\"item\":\"19\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":1},{\"item\":\"18\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":2},{\"item\":\"16\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":3},{\"item\":\"12\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":4},{\"item\":\"23\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":5},{\"item\":\"33\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":6},{\"item\":\"5\",\"item_type\":\"sku_id\",\"pos_id\":4,\"pos_seq\":7}],\"page\":{\"during_time\":15484,\"from_pos_id\":8,\"from_pos_seq\":15,\"item\":\"16\",\"item_type\":\"sku_id\",\"last_page_id\":\"home\",\"page_id\":\"good_detail\"},\"ts\":1731297892669}","id":24862},
// "source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"z_log"},
// "ts_ms":1734933950700}
        ReadOnlyBroadcastState<String, JSONObject> Rbs = readOnlyContext.getBroadcastState(mapStateDescriptor);
//        System.out.println("Rbs:"+Rbs);
//HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo
// {name='mapState', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@49b0b76, valueSerializer=org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer@28357053, assignmentMode=BROADCAST},
// backingMap={}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@b6fbd39d}
        String tableName = jsonObject.getJSONObject("source").getString("table");
//        System.out.println("source_tableName==>"+tableName);
        JSONObject jsonObject1 = Rbs.get(tableName);
//        jsonObject1=null

        if(jsonObject1!=null || configMap.get(tableName)!=null){
            if (configMap.get(tableName).getSourceTable().equals(tableName)) {
//                System.out.println("jsonObject1==>" + jsonObject)
//{"op":"r",
// "after":{"create_time":1639440000000,"name":"音乐","id":5,"category2_id":2},
// "source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"base_category3"},
// "ts_ms":1734936713793}
                if(!jsonObject.getString("op").equals("d")){
                    JSONObject data = jsonObject.getJSONObject("after");
                    String sinkTable = configMap.get(tableName).getSinkTable();
                    System.out.println("sinkTable===>"+sinkTable);
                    sinkTable="gmall:"+sinkTable;
                    String rowKey = data.getString(configMap.get(tableName).getSinkRowKey());
                    System.out.println("rowKey===>"+rowKey);
                    Table table = hbaseConnection.getTable(TableName.valueOf(sinkTable));
                    System.out.println("table===>"+table);
                    Put put = new Put(Bytes.toBytes(rowKey));
                    for (Map.Entry<String, Object> entry : data.entrySet()) {
                        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(String.valueOf(entry.getValue())));
                        System.out.println(table+"添加数据中....");
                    }
                    table.put(put);

                }
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//        System.out.println("pbe:"+jsonObject);
//{"op":"r",
//"after":{"sink_row_key":"dic_code","sink_family":"info","sink_table":"dim_base_dic","source_table":"base_dic","sink_columns":"dic_code,dic_name"}}
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        if(jsonObject.containsKey("after")){
            String tables = jsonObject.getJSONObject("after").getString("sink_table");
            if("d".equals(op)){
                broadcastState.remove(tables);
            }else{
                broadcastState.put(tables,jsonObject);
                System.out.println(tables+"添加数据中....");
            }
        }

    }
    @Override
    public void close() throws Exception {
        super.close();
        hbaseConnection.close();
    }
}



//        System.out.println("configMap==>"+configMap);
//{
//spu_info=TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=null),
//base_trademark=TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=null),
//sku_info=TableProcessDim(sourceTable=sku_info, sinkTable=dim_sku_info, sinkColumns=id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time, sinkFamily=info, sinkRowKey=id, op=null),
//activity_info=TableProcessDim(sourceTable=activity_info, sinkTable=dim_activity_info, sinkColumns=id,activity_name,activity_type,activity_desc,start_time,end_time,create_time, sinkFamily=info, sinkRowKey=id, op=null),
//base_province=TableProcessDim(sourceTable=base_province, sinkTable=dim_base_province, sinkColumns=id,name,region_id,area_code,iso_code,iso_3166_2, sinkFamily=info, sinkRowKey=id, op=null),
//financial_sku_cost=TableProcessDim(sourceTable=financial_sku_cost, sinkTable=dim_financial_sku_cost, sinkColumns=id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time, sinkFamily=info, sinkRowKey=id, op=null),
//base_category2=TableProcessDim(sourceTable=base_category2, sinkTable=dim_base_category2, sinkColumns=id,name,category1_id, sinkFamily=info, sinkRowKey=id, op=null),
//base_category3=TableProcessDim(sourceTable=base_category3, sinkTable=dim_base_category3, sinkColumns=id,name,category2_id, sinkFamily=info, sinkRowKey=id, op=null),
//base_category1=TableProcessDim(sourceTable=base_category1, sinkTable=dim_base_category1, sinkColumns=id,name, sinkFamily=info, sinkRowKey=id, op=null),
//base_region=TableProcessDim(sourceTable=base_region, sinkTable=dim_base_region, sinkColumns=id,region_name, sinkFamily=info, sinkRowKey=id, op=null),
//coupon_info=TableProcessDim(sourceTable=coupon_info, sinkTable=dim_coupon_info, sinkColumns=id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc, sinkFamily=info, sinkRowKey=id, op=null),
//activity_rule=TableProcessDim(sourceTable=activity_rule, sinkTable=dim_activity_rule, sinkColumns=id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level, sinkFamily=info, sinkRowKey=id, op=null),
//user_info=TableProcessDim(sourceTable=user_info, sinkTable=dim_user_info, sinkColumns=id,login_name,name,phone_num,email,user_level,birthday,gender,create_time,operate_time, sinkFamily=info, sinkRowKey=id, op=null),
//activity_sku=TableProcessDim(sourceTable=activity_sku, sinkTable=dim_activity_sku, sinkColumns=id,activity_id,sku_id,create_time, sinkFamily=info, sinkRowKey=id, op=null),
//base_dic=TableProcessDim(sourceTable=base_dic, sinkTable=dim_base_dic, sinkColumns=dic_code,dic_name, sinkFamily=info, sinkRowKey=dic_code, op=null),
//coupon_range=TableProcessDim(sourceTable=coupon_range, sinkTable=dim_coupon_range, sinkColumns=id,coupon_id,range_type,range_id, sinkFamily=info, sinkRowKey=id, op=null)
// }