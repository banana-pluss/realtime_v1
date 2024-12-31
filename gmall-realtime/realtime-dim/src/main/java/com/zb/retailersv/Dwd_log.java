package com.zb.retailersv;

import com.alibaba.fastjson.JSONObject;
import com.zb.retailersv.func.ProcessSpiltStream;
import com.zbm.util.CommonUtils;
import com.zbm.util.ConfigUtils;
import com.zbm.util.DateTimeUtils;
import com.zbm.util.KafkaUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;


public class Dwd_log {
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String realtime_kafka_log_topic = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_topic_err = ConfigUtils.getString("kafka.topic.err");
    private static final String kafka_topic_start = ConfigUtils.getString("kafka.topic.start");
    private static final String kafka_topic_page = ConfigUtils.getString("kafka.topic.page");
    private static final String kafka_topic_action = ConfigUtils.getString("kafka.topic.action");
    private static final String kafka_topic_display = ConfigUtils.getString("kafka.topic.display");
    private static final String kafka_topic_NdjsonTag = "topic_NdjsonTag";
    private static final OutputTag<String> NdjsonTag = new OutputTag<String>("NdjsonTag") {
    };
    private static final OutputTag<String> errTage = new OutputTag<String>("errTage") {
    };
    private static final OutputTag<String> startTag = new OutputTag<String>("startTage") {
    };
    private static final OutputTag<String> displaysTag = new OutputTag<String>("displaysTag") {
    };
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
    };
    private static final HashMap<String, DataStream<String>> collectDsMap = new HashMap<>();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        CommonUtils.printCheckPropEnv(
                false,
                kafka_bootstrap_servers,
                realtime_kafka_log_topic,
                kafka_topic_err,
                kafka_topic_start,
                kafka_topic_display,
                kafka_topic_page,
                kafka_topic_action
        );


        DataStreamSource<String> kafka_topic_log = env.fromSource(KafkaUtils.buildKafkaSource(kafka_bootstrap_servers, realtime_kafka_log_topic, "flink-consumer-log", OffsetsInitializer.earliest()), WatermarkStrategy.noWatermarks(), "kafka_topic_log");
//13> {"common":{"ar":"7","ba":"OPPO","ch":"xiaomi","is_new":"0","md":"OPPO Remo8","mid":"mid_249","os":"Android 13.0","sid":"b3711a2f-bc81-4d2a-b872-78ac8a317e2b","uid":"1892","vc":"v2.1.134"},"page":{"during_time":8545,"item":"8970","item_type":"order_id","last_page_id":"order","page_id":"payment"},"ts":1731597872643}
//        kafka_topic_log.print();
        SingleOutputStreamOperator<JSONObject> processlog = kafka_topic_log.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                try {
                    collector.collect(JSONObject.parseObject(s));
                } catch (Exception e) {
                    context.output(NdjsonTag, s);
                }

            }
        }).uid("string_to_json").name("string_to_json");
//13> {"common":{"ar":"24","uid":"341","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"OPPO Remo8","mid":"mid_19","vc":"v2.1.134","ba":"OPPO","sid":"05f9f0ad-f5ce-40d2-96d4-357f70717a6b"},"page":{"page_id":"order","item":"9,15,2","during_time":14958,"item_type":"sku_ids","last_page_id":"cart"},"ts":1731596508909}
//        processlog.print();
        processlog.getSideOutput(NdjsonTag).print("NdjsonTag - >:");
        processlog.getSideOutput(NdjsonTag).sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_topic_NdjsonTag)).uid("sink_to_kafka").name("sink_to_kafka");

        KeyedStream<JSONObject, String> keybylog = processlog.keyBy(x -> x.getJSONObject("common").getString("mid"));
//        keybylog.print();

        SingleOutputStreamOperator<JSONObject> maplog = keybylog.map(new RichMapFunction<JSONObject, JSONObject>() {
            ValueState<String> state;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                state = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isnew = jsonObject.getJSONObject("common").getString("is_new");
                String value = state.value();
                Long ts = jsonObject.getLong("ts");
                String format = DateTimeUtils.tsToDate(ts);
                //is_new为1
                if ("1".equals(isnew)) {
                    //判断state是否为空,为空的话代表新用户
                    if (StringUtils.isEmpty(value)) {
                        state.update(format);
                    } else if (value.equals(format)) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                    //如果is_new为0
                } else {
                    //且StringUtils等于null,那么代表老用户第一次进入此页面
                    if (StringUtils.isEmpty(value)) {
                        state.update(DateTimeUtils.tsToDate(ts - 24 * 60 * 60 * 1000));
                    }
                }
                return jsonObject;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }).uid("filter_to_topic_log").name("filter_to_topic_log");
//        maplog.print("maplog - >:");

        SingleOutputStreamOperator<String> processlogs = maplog.process(new ProcessSpiltStream(errTage, displaysTag, startTag, actionTag))
                .uid("kafka_to_tage")
                .name("kafka_to_tage");
//26> {"common":{"ar":"33","uid":"941","os":"iOS 13.2.9","ch":"Appstore","is_new":"0","md":"iPhone 13","mid":"mid_120","vc":"v2.1.134","ba":"iPhone","sid":"d766b6a0-24a4-40b2-bcf7-d62e2eb51bec"},"page":{"page_id":"order_list","during_time":10057,"last_page_id":"mine"},"ts":1731580385479}
        processlogs.print();
        SideOutputDataStream<String> erroutDs = processlogs.getSideOutput(errTage);
        SideOutputDataStream<String> displaysDs = processlogs.getSideOutput(displaysTag);
        SideOutputDataStream<String> startDs = processlogs.getSideOutput(startTag);
        SideOutputDataStream<String> actionDs = processlogs.getSideOutput(actionTag);

        //collectDsMap(String,DataStream<String>)
        collectDsMap.put("err", erroutDs);
        collectDsMap.put("display", displaysDs);
        collectDsMap.put("start", startDs);
        collectDsMap.put("action", actionDs);
        collectDsMap.put("page", processlogs);
//        side2kafkatopic(collectDsMap);


        env.execute();

    }

    public static void side2kafkatopic(HashMap<String, DataStream<String>> collectDsMap) {
        collectDsMap.get("err").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_topic_err))
                .uid("sink_to_kafka_err")
                .name("sink_to_kafka_err");

        collectDsMap.get("display").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_topic_display))
                .uid("sink_to_kafka_display")
                .name("sink_to_kafka_display");

        collectDsMap.get("start").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_topic_start))
                .uid("sink_to_kafka_start")
                .name("sink_to_kafka_start");

        collectDsMap.get("action").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_topic_action))
                .uid("sink_to_kafka_action")
                .name("sink_to_kafka_action");

        collectDsMap.get("page").sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_topic_page))
                .uid("sink_to_kafka_page")
                .name("sink_to_kafka_page");

    }
}
