package myapp;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.*;

public class StreamClient {

    public static void main(String[] args) throws Exception {
        FileInputStream fi = new FileInputStream("test.properties");

    	Properties props = new Properties();
    	
    	props.load(fi);
        
    	String filter1 = props.getProperty("filter.one");
    	String filter2 = props.getProperty("filter.two");

       // props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-client");
       // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.195:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        final StreamsBuilder builder = new StreamsBuilder();
        

        String sourceTopic = props.getProperty("source.topic","source-stream-input");
        KStream<String, String> source = builder.stream(sourceTopic); 
        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
        words.to("streams-linesplit-output");
        
        
        /**  java 7 uses following  instead of ->
        KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
        	public Iterable<String> apply(String value){
        		return Arrays.asList(value.split("\\W+"));
        	}
        });
        */


        //builder.stream("streams-plaintext-input").to("streams-pipe-output");
        source.peek((k,v) ->System.out.print(v)).to("streams-output");
        
        
        source.split()
        		.branch(
        				(key, value) -> value.contains(filter1),
        				Branched.withConsumer(ks->ks.peek((k,v) ->System.out.print(v)).to("Branch-1")))
        		.branch(
        				(key,value)->value.contains(filter2),
        				Branched.withConsumer(ks->ks.to("Branch-2")))
        		.branch(
        				(key,value)-> true,
        				Branched.withConsumer(ks->ks.to("Branch-3")));
        		
        		
 
        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}


