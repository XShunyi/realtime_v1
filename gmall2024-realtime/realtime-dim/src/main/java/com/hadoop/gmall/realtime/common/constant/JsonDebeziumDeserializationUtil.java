package com.hadoop.gmall.realtime.common.constant;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/*
SourceRecord{sourcePartition={server=mysql_binlog_source},
sourceOffset={file=mysql-bin.000008, pos=13289, row=1, snapshot=true}}
ConnectRecord{topic='mysql_binlog_source.mydb.t_a', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{mysql_binlog_source.mydb.t_a.Key:STRUCT},
value=Struct{after=Struct{id=1,name=zhangsan,age=13},
source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=mydb,table=t_a,server_id=0,file=mysql-bin.000008,pos=13289,row=0},
op=c,ts_ms=1702974583658}, valueSchema=Schema{mysql_binlog_source.mydb.t_a.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

{db: ,tb:, data:{id: ,name: age:},op:}
 */
public class JsonDebeziumDeserializationUtil implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        String[] topic = sourceRecord.topic().split("\\.");
        //获取数据库名称
        String db = topic[1];
        //获取表名
        String tb = topic[2];
        //data对象封装value-after数据
        JSONObject data = new JSONObject();
        Struct value = (Struct)sourceRecord.value();
        Struct after = value.getStruct("after");
        if(after != null){
            //获取after的所有字段名称集合
            List<Field> fields = after.schema().fields();
            for(Field f:fields){
                String fn = f.name(); //id name age
                Object fv = after.get(fn);
                //放入json
                data.put(fn,fv);
            }
        }
        Object op = value.get("op");
        JSONObject result = new JSONObject();
        //放入json
        result.put("db",db);
        result.put("tb",tb);
        result.put("op",op);
        result.put("data",data);

        collector.collect(result.toString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
