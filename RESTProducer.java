package myapp;

import java.io.FileInputStream;
//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;



//Create java class named ¡°SimpleProducer¡±
public class RESTProducer {
 
 public static void main(String[] args) throws Exception{
    
    FileInputStream fi = new FileInputStream("test.properties");
 	Properties props = new Properties();
 	props.load(fi);
    
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");            
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 
    String topicName = props.getProperty("source.topic","source-stream-input");
	String filter1 = props.getProperty("filter.one");
	String filter2 = props.getProperty("filter.two");

    Producer<String, String> producer = new KafkaProducer<>(props);
          
    for(int i = 0; i < 3; i++) {
       producer.send(new ProducerRecord<String, String>(topicName, filter1 + Integer.toString(i) +  " message added successfully" ));
       producer.send(new ProducerRecord<String, String>(topicName, filter2 + Integer.toString(i) +  " message added successfully" ));
       producer.send(new ProducerRecord<String, String>(topicName, "Something else" + Integer.toString(i) +  " message added successfully" ));

    }
     System.out.println("Message sent successfully");
     producer.close();
 }
}