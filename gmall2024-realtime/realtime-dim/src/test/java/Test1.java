import com.atguigu.gmall.realtime.common.constant.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置并行度
        //创建消费者对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim01")
                // 从头开始消费
                .setStartingOffsets(OffsetsInitializer.earliest())
                //从最末尾开始消费
                //.setStartingOffsets(OffsetsInitializer.latest())
                //如果使用flink提供的SimpleStringSchema对string类型的消息反序列化  , 如果消息为空会报错
                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setValueOnlyDeserializer(
//                        new DeserializationSchema<String>() {
//                            @Override
//                            public String deserialize(byte[] bytes) throws IOException {
//                                if (bytes != null) {
//                                    return new String(bytes);
//                                }
//                                return null;
//                            }
//
//                            @Override
//                            public boolean isEndOfStream(String s) {
//                                return false;
//                            }
//
//                            @Override
//                            public TypeInformation<String> getProducedType() {
//                                return TypeInformation.of(String.class);
//                            }
//                        }
//                )
                .build();
        //
        DataStreamSource<String> kafka_source =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        kafka_source.print();




        env.execute();



    }
}
