package com.sy.util;



import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> dataStreamSource);
    public void start(String topicDb,String groupId,int p,int port)  {
        // 1. 设置Hadoop执行用户
        System.setProperty("HADOOP_USER_NAME", "root");
        // 2. 创建流环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 3.设置并行度
        env.setParallelism(p);
        // 4.设置CK
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh02:8020/gmall2024_config/stream/"+groupId );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 5.消费数据
        DataStreamSource<String> dataStreamSource = env.fromSource(FlinkSourceUtil.getKafkaSource(topicDb, groupId), WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 6.打印数据
//        dataStreamSource.print();
        handle(env,dataStreamSource);

        // 7.执行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
