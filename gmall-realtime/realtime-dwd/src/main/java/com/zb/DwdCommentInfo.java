package com.zb;

import com.alibaba.fastjson.JSONObject;
import com.zbm.util.ConfigUtils;
import com.zbm.util.EnvironmentSettingUtils;
import com.zbm.util.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DwdCommentInfo {
    private static final String topic_db = ConfigUtils.getString("kafka.topic.db");
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String topic_comment_info = ConfigUtils.getString("topic.comment.info");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        DataStreamSource<String> kafka_topic_db = env.fromSource(KafkaUtils.buildKafkaSource(kafka_bootstrap_servers, topic_db, "flink-consumer-log", OffsetsInitializer.earliest()), WatermarkStrategy.noWatermarks(), "kafka_topic_log");
        SingleOutputStreamOperator<JSONObject> jsonmapdb = kafka_topic_db.map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> flatMapdb = jsonmapdb.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<JSONObject> collector) throws Exception {
                        if (jsonObject.getJSONObject("source").getString("table").equals("comment_info")) {
                            collector.collect(jsonObject);
                        }
                    }
                }).uid("kafka_to_comment_info")
                .name("kafka_to_comment_info");
//        flatMapdb.print();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE base_dic (" +
                " rowkey STRING," +
                " info ROW<dic_name STRING,parent_code STRING>," +
                " PRIMARY KEY (rowkey) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'gmall:dim_base_dic'," +
                " 'zookeeper.quorum' = 'cdh01:2181'" +
                ")");
//        tenv.sqlQuery("select * from base_dic").execute().print();

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
        Table comment_info = tenv.sqlQuery("select " +
                "after['id'] id," +
                "after['user_id'] user_id," +
                "after['nick_name'] nick_name, " +
                "after['head_img'] head_img, " +
                "after['sku_id'] sku_id, " +
                "after['spu_id'] spu_id, " +
                "after['order_id'] order_id, " +
                "after['appraise'] appraise, " +
                "after['comment_txt'] comment_txt, " +
                "after['create_time'] create_time, " +
                "after['operate_time'] operate_time, " +
                "proc_time " +
                " from topic_db where source['table']='comment_info'" +
                " and op='r' and source['db']='gmall'");

        tenv.createTemporaryView("comment_info",comment_info);
        Table table = tenv.sqlQuery("select " +
                " id, " +
                " user_id, " +
                " nick_name, " +
                " sku_id, " +
                " spu_id, " +
                " order_id, " +
                " appraise, " +
                " info.dic_name, " +
                " comment_txt, " +
                " create_time " +
                " from comment_info as c  join base_dic FOR SYSTEM_TIME AS OF c.proc_time AS b" +
                " on c.appraise=b.rowkey");

//        table.execute().print();

        DataStream<Row> rowDataStream = tenv.toDataStream(table);
//        SingleOutputStreamOperator<Object> map = rowDataStream.map(JSONObject::toJSON);
        SingleOutputStreamOperator<Object> map = rowDataStream.map(String::valueOf);
//        SingleOutputStreamOperator<String> map1 = map.map(JSONObject::toJSONString);
        map.print();
//        map.sinkTo(2> "+I[895, 2184, 星光, 21, 6, 8079, 1201, 好评, 评论内容：24438798126147361152512822885433473969453677911366, 1731584641000]"
//                KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,topic_comment_info)
//        );

        env.execute();
    }
}
