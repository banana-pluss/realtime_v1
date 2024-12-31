package com.zb;

import com.zbm.util.ConfigUtils;
import com.zbm.util.EnvironmentSettingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdOrderDetail {
    private static final String topic_db = ConfigUtils.getString("kafka.topic.db");

    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("CREATE TABLE topic_db (" +
                "  op string," +
                "before map<String,String>," +
                "after map<String,String>," +
                "source map<String,String>," +
                "ts_ms bigint," +
                "row_time as TO_TIMESTAMP_LTZ(ts_ms,3)," +
                "proc_time as proctime(), " +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic_db + "'," +
                "  'properties.bootstrap.servers' = '" + kafka_bootstrap_servers + "'," +
                "  'properties.group.id' = '1'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")");





        env.execute();
    }
}
