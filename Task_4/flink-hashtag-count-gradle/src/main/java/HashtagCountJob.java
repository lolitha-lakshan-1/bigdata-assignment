import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.time.Duration;
import java.util.Objects;

public class HashtagCountJob {

    private static final String KAFKA_SERVER = "kafka-task4:9092";
    private static final String TWITTER_TOPIC = "twitter.topic";
    private static final String TIKTOK_TOPIC = "tiktok.topic";
    private static final String GROUP_ID = "GROUP_ID";
    
    private static final int WINDOW_SECONDS = 15;
    private static final int WATERMARK_DELAY_SECONDS = 5;
    
    private static final String TWITTER_HASHTAG = "#GloboNews";
    private static final String TIKTOK_HASHTAG = "#sae";

    private static final String TWITTER_JOB_NAME = "twitter-hashtag-job";
    private static final String TIKTOK_JOB_NAME = "tiktok-hashtag-job";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        environment.getConfig().setGlobalJobParameters(parameters);

        String jobName = null;
        String topic = null;
        String groupId = null;
        String hashtag = null;

        if (parameters.get("jobType").equals("TWITTER")) {
            jobName = TWITTER_JOB_NAME + parameters.get("jobId");
            topic = TWITTER_TOPIC;
            groupId = GROUP_ID;
            hashtag = TWITTER_HASHTAG;
        } else {
            jobName = TIKTOK_JOB_NAME + parameters.get("jobId");
            topic = TIKTOK_TOPIC;
            groupId = GROUP_ID;
            hashtag = TIKTOK_HASHTAG;
        }

        DataStream<String> kafkaStream = getKafkaStream(environment, topic, groupId);
        DataStream<Event> parsedEvents = kafkaStream.map(EventParser::parse).filter(Objects::nonNull);
        DataStream<Event> watermarkedEvents = addWatermarksToEvents(parsedEvents);
        
        DataStream<Result> resultStream =
            watermarkedEvents
                .keyBy(event -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
                .process(new WindowResultFunction(hashtag, jobName))
                .filter(result -> result.getHashtagEventCount() > 0);


        resultStream.addSink(PostgresResultSink.createResultSink());
        environment.execute(jobName);
    }

    private static DataStream<String> getKafkaStream(
        StreamExecutionEnvironment environment,
        String topic,
        String groupId) {

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_SERVER)
            .setTopics(topic)
            .setGroupId(groupId)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .build();

        return environment.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "twitter-kafka-source"
        );
    }


    private static DataStream<Event> addWatermarksToEvents(DataStream<Event> parsedEvents) {
        
        parsedEvents = parsedEvents.filter(event -> event.getEventTimeMillis() > 0);

        WatermarkStrategy<Event> strategy =
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(WATERMARK_DELAY_SECONDS))
                        .withTimestampAssigner(
                                (event, previousTimestamp) ->
                                        event.getEventTimeMillis()
                        );

        return  parsedEvents.assignTimestampsAndWatermarks(strategy);
    }

}
