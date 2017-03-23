import java.util.*;
import java.util.function.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import scala.Tuple3;
import org.apache.spark.streaming.kafka010.*;

public class WordCountTask {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String REDIS_KEY = "counters_(s_w_l):";

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: WordCountTask <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n" +
                    "  <redisIp> is ip of redis server\n" +
                    "  <redisPort> is port of redis server\n");
            System.exit(1);
        }

        final String brokers = args[0];
        final String topics = args[1];
        final String redisIp = args[2];
        final int redisPort = Integer.parseInt(args[3]);

        JedisPoolHolder.init(redisIp, redisPort);

        SparkConf sparkConf = new SparkConf().setAppName("WordCountTask");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group_id");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topics.split(",")), kafkaParams)
        );

        String s = " dsdfsdf";

        final JavaDStream<Tuple3<Integer, Integer, Integer>> mapReduce = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Iterator<Tuple3<Integer, Integer, Integer>> call(ConsumerRecord<String, String> record) throws Exception {
                return Stream.of(SPACE.split(record.value())).map(new Function<String, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> apply(String s) {
                        return new Tuple3<>(Counter.sentences.apply(s), Counter.words.apply(s), Counter.lattes.apply(s));
                    }
                }).iterator();
            }
        }).reduce(new Function2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> call(Tuple3<Integer, Integer, Integer> t1, Tuple3<Integer, Integer, Integer> t2) {
                return new Tuple3<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3());
            }
        });

        mapReduce.foreachRDD(rdd -> rdd.foreachPartition(p -> p.forEachRemaining(new Consumer<Tuple3<Integer, Integer, Integer>>() {
                                                                                     @Override
                                                                                     public void accept(Tuple3<Integer, Integer, Integer> tuple) {
                                                                                         Jedis jedis = JedisPoolHolder.getInstance().getResource();
                                                                                         jedis.set(REDIS_KEY + tuple.hashCode(), tuple.toString());
                                                                                         JedisPoolHolder.getInstance().returnResourceObject(jedis);
                                                                                     }
                                                                                 }
        )));
        jssc.start();
        jssc.awaitTermination();
    }
}
