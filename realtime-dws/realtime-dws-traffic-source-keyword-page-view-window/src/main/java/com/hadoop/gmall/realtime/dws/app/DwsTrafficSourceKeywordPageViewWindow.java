package com.hadoop.gmall.realtime.dws.app;

import com.bw.gmall.realtime.common.base.BaseSqlApp;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.function.KwSplit;
import com.bw.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW, 1, 10021);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        // 1. 读取 页面日志
        tableEnv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second " +
                ")" + SQLUtil.getKafkaSourceSQL( Constant.TOPIC_DWD_TRAFFIC_PAGE,"1"));

        // 2. 读取搜索关键词
        Table kwTable = tableEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("kw_table", kwTable);
//        kwTable.execute().print();

        // 3. 自定义分词函数
        tableEnv.createTemporaryFunction("kw_split", KwSplit.class);


        Table keywordTable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true ");
        tableEnv.createTemporaryView("keyword_table", keywordTable);
//        keywordTable.execute().print();


        // 3. 开窗聚和
        Table result = tableEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, '2024-11-20') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '5' second ) ) " +
                "group by window_start, window_end, keyword ");
//        result.execute().print();
        // 5. 写出到 doris 中
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.FENODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = 'root', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}