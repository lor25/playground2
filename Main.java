package main;

import config.AppConfig;
import java.util.*;

import org.apache.spark.streaming.Duration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Main {


    private static final String SENTENCE_PT = "(.*?[!?.]+)(?:\\s)";
    private static final String WORD_PT = " ";
    RedisTemplate redisTemplate;


    public static void main(String arg[]) throws Exception {

        new Main().runKafkaStreaming(new AnnotationConfigApplicationContext(AppConfig.class));
    }

    /*
    * Hardcoded config for testing purposes
    *
    *
    */
    public void runKafkaStreaming(AnnotationConfigApplicationContext ctx) throws Exception{


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        redisTemplate = (StringRedisTemplate) ctx.getBean("stringRedisTemplate");
        Set<String> topicSet = new HashSet<>(Arrays.asList("words"));
        Map<String, String> kafkaParams = new HashMap<>();

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("NetworkWordCount");

        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("group.id", "my-group");


        JavaStreamingContext context = new JavaStreamingContext(conf, Duration.apply(3000));

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                context,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicSet
        );


        JavaDStream<String> lines = messages.map(Tuple2::_2);

        lines.filter(s -> !s.isEmpty()).flatMap(x -> Arrays.asList(x.split(WORD_PT)).iterator()).count().
                foreachRDD(x -> redisTemplate.opsForValue().increment("WORDS", x.rdd().first()));

        lines.filter(s -> !s.trim().isEmpty())
                .flatMap(x -> Arrays.asList(x.split(SENTENCE_PT))
                        .iterator()).count()
                .foreachRDD(x -> redisTemplate.opsForValue().increment("SENTENCES", x.rdd().first()));

        lines.filter(s -> !s.isEmpty()).flatMap(x -> Arrays.asList(x
                .replace(" ", "")
                .replace(".", "")
                .replace("!", "")
                .replace("?", "")
                .split(""))
                .iterator()).count()
                .foreachRDD(x -> redisTemplate.opsForValue().increment("LETTERS", x.rdd().first()));


        lines.print();
        context.start();
        context.awaitTermination();


    }



}
