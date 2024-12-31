package com.zb.retailersv;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.zb.retailersv.func.ProcessSpiltStreamdb;
import com.zb.stream.utils.CdcSourceUtils;
import com.zbm.util.ConfigUtils;
import com.zbm.util.EnvironmentSettingUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Dwd_db {
    private static final String mysql_database = ConfigUtils.getString("mysql.database");
    private static final String mysql_database_conf = ConfigUtils.getString("mysql.databases.conf");
    private static final String mysql_user = ConfigUtils.getString("mysql.user");
    private static final String mysql_pwd = ConfigUtils.getString("mysql.pwd");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                mysql_database,
                "",
                mysql_user,
                mysql_pwd,
                StartupOptions.initial()
        );

        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                mysql_database_conf,
                  "gmall_config.table_process_dwd",
                mysql_user,
                mysql_pwd,
                StartupOptions.initial()
        );

        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dwd_source");

        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_convert_json")
                .name("db_convert_json")
                .setParallelism(1);

        SingleOutputStreamOperator<JSONObject> cdcDbdwdStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dwd_data_convert_json")
                .name("dw_data_convert_json")
                .setParallelism(1);

        SingleOutputStreamOperator<JSONObject> cdcDbDwdStreamMapCleanColumn = cdcDbdwdStreamMap.map(s -> {
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");


        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = cdcDbDwdStreamMapCleanColumn.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = cdcDbMainStreamMap.connect(broadcastDs);
        connectDs.process(new ProcessSpiltStreamdb(mapStageDesc));
        env.execute();
    }
}
