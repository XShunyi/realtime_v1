package com.bw.gmall.realtime.common.base;

import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.FlinkSourceUtil;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSqlApp {

    public void start(String groupId,int p,int port)  {
        // 1. 设置Hadoop执行用户
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 2. 创建流环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        StreamTableEnvironment tableEnv  = StreamTableEnvironment.create(env);
        // 3.设置并行度
        env.setParallelism(p);
        // 4.设置CK
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/_config/stream/"+groupId );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 5.消费数据

        // 6.打印数据
//        dataStreamSource.print();
        handle(env,tableEnv,groupId);

//        // 7.执行
////        try {
////            env.execute();
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
    }

    public abstract void handle(StreamExecutionEnvironment env,StreamTableEnvironment tableEnv, String groupId);


    // 子类调用
    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId){
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(groupId));
    }

    //读取HBase的base_dic字典表
    public void createBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '"+ Constant.HBASE_ZOOKEEPER_QUORUM+"'\n" +
                ")");
    }

}
