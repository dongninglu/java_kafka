package myapp;


import java.util.Properties;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RESTConsumer {
   public static void main(String[] args) throws Exception {
    
	  final String topic1 = "Branch-1";
	  final String topic2 = "Branch-2";
	  final String topic3 = "Branch-3";
      Properties props = new Properties();
      FileInputStream fi = new FileInputStream("test.properties");
      props.load(fi);

      props.put("group.id", "RESTConsumer");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props);
      KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props);
      KafkaConsumer<String, String> consumer3 = new KafkaConsumer<>(props);

      
      consumer1.subscribe(Collections.singletonList(topic1));
      consumer2.subscribe(Collections.singletonList(topic2));
      consumer3.subscribe(Collections.singletonList(topic3));
      
      System.out.println("Subscribed to topics " + topic1 + "  +  " + topic2 + "   +  " + topic3);
     
         
      while (true) {
        ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofMillis(1000));
  		int nn1=records1.count();
  		System.out.println("records1 nn is " + nn1);
  		
            for (ConsumerRecord<String, String> record : records1)
               System.out.printf(topic1 +  "  offset = %d, key = %s, value = %s\n", 
               record.offset(), record.key(), record.value());

        ConsumerRecords<String, String> records2 = consumer2.poll(Duration.ofMillis(1000));
        int nn2=records2.count();
 		System.out.println("records2 nn is " + nn2);

            for (ConsumerRecord<String, String> record : records2)
               System.out.printf(topic2 +  "  offset = %d, key = %s, value = %s\n", 
               record.offset(), record.key(), record.value());
            
        ConsumerRecords<String, String> records3 = consumer3.poll(Duration.ofMillis(1000));
		int nn3=records3.count();
 		System.out.println("records3 nn is " + nn3);

            for (ConsumerRecord<String, String> record : records3)
               System.out.printf(topic3 +  "  offset = %d, key = %s, value = %s\n", 
               record.offset(), record.key(), record.value());

      }     
   }  
}