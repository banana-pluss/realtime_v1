package com.zb.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.zb.retailersv.domain.TableProcessDim;
import com.zbm.util.ConfigUtils;
import com.zbm.util.HbaseUtils;
import com.zbm.util.JdbcUtils;
import com.zbm.util.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.jcodings.util.Hash;

import java.sql.Connection;
import java.util.*;

public class ProcessSpiltStreamdb extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {
    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap = new HashMap<>();
    private org.apache.hadoop.hbase.client.Connection hbaseConnection;

    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    private HbaseUtils hbaseUtils;

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(ConfigUtils.getString("mysql.url"), ConfigUtils.getString("mysql.user"), ConfigUtils.getString("mysql.pwd"));
        String querySQL = "select * from gmall_config.table_process_dwd";
        /**
         * 也可以封装成JSONObject
         */
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        // configMap:spu_info -> TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=null)
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        connection.close();
        hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseConnection = hbaseUtils.getConnection();
    }

    public ProcessSpiltStreamdb(MapStateDescriptor<String, JSONObject> mapStageDesc) {
        this.mapStateDescriptor = mapStageDesc;
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
//jsonObject.toString - >:{"op":"r","after":{"payment_way":"3501","refundable_time":1731954100000,"original_total_amount":"D0JA","order_status":"1004","consignee_tel":"13218631878","trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米等7件商品","id":112,"operate_time":1731349357000,"consignee":"穆素云","create_time":1731349300000,"coupon_reduce_amount":"AA==","out_trade_no":"236316125727215","total_amount":"D0JA","user_id":26,"province_id":4,"activity_reduce_amount":"AA=="},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"order_info"},"ts_ms":1735130262873}
        //        System.out.println("jsonObject.toString - >:"+jsonObject.toString());
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
//        System.out.println(broadcastState);
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);
        System.out.println(broadData);
        if (broadData != null || configMap.get(tableName) != null) {
            if (configMap.get(tableName).getSourceTable().equals(tableName)) {
//                System.err.println(jsonObject);
                if (!jsonObject.getString("op").equals("d")) {
                    String table = jsonObject.getJSONObject("source").getString("table");
                    String tableNames ;
                    if (jsonObject.getString("op").equals("u")) {
                        tableNames = "topic_" + table + "_u";
                    } else {
                        tableNames = "topic_" + table;
                        //Properties 通常用于配置客户端参数
                    }
//                    System.out.println(tableNames);
//                    Properties adminProps = new Properties();
//                    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_bootstrap_servers);
//                    //AdminClient 是使用完及释放资源,还可以创建以及删除topic
//                    try (AdminClient adminClient = AdminClient.create(adminProps)) {
//                        //
//                        NewTopic newTopic = new NewTopic(tableNames, 1, (short) 3); // 1 partition, 1 replica
//                        try {
//                            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
//                            System.out.println("Topic created: " + tableNames);
//                        } catch (Exception e) {
//                            System.out.println("Topic already exists: " + tableNames);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    KafkaUtils.sendDataToKafka(tableNames, jsonObject);
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@39529185, valueSerializer=org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer@59b0797e, assignmentMode=BROADCAST}, backingMap={}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@4ab01899}
        String op = jsonObject.getString("op");
        if (jsonObject.containsKey("after")) {
            String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");
            if ("d".equals(op)) {
                broadcastState.remove(sourceTableName);
            } else {
                broadcastState.put(sourceTableName, jsonObject);
//                configMap.put(sourceTableName,jsonObject.toJavaObject(TableProcessDim.class));
            }
        }
    }
}
