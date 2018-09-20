package apacheStorm.spouts;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FakeCallLogReaderSpout implements IRichSpout {
	
	public Map<String, Object> getComponentConfiguration() {
		System.out.println(">>>>>>>>>>>>Map method of Spout>>>>>>>>>>>>");
		return null;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println(">>>>>>>>>>>>OutputFieldsDeclarer declarer>>>>>>>>>>>>");
		declarer.declare(new Fields("MyColumn1"));		
	}
	//Create instance for SpoutOutputCollector which passes tuples to bolt.
	private SpoutOutputCollector collector;

	//Create instance for TopologyContext which contains topology data.
	private TopologyContext context;

	private KafkaConsumer<Integer, String> consumer;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		System.out.println(">>>>>>>>>>>>open method of Spout>>>>>>>>>>>>");
		this.context = context;
		this.collector = collector;
	}

	public void activate() {
		System.out.println(">>>>>>>>>>>>activate method of Spout>>>>>>>>>>>>");
	}
	
	public void nextTuple() {
			//Kafka consumer configuration settings
		System.out.println(">>>>>>>>>>>>nextTuple method of Spout>>>>>>>>>>>>");
			String topicName = "imagetext";//args[0].toString();
			Properties props = new Properties();
			props.put("bootstrap.servers", "victoria.com:6667");
			props.put("metadata.broker.list", "victoria.com:6667");
	        props.put("zookeeper.connect", "victoria.com:2181");
			props.put("group.id", "test");
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("session.timeout.ms", "30000");
			props.put("key.deserializer", 
					"org.apache.kafka.common.serialization.IntegerDeserializer");
			props.put("value.deserializer", 
					"org.apache.kafka.common.serialization.StringDeserializer");
			consumer = new KafkaConsumer
					<Integer, String>(props);

			//Kafka Consumer subscribes list of topics here.
			consumer.subscribe(Arrays.asList(topicName));

			//print the topic name
			System.out.println(">>>>>>>>>>>>Subscribed to topic>>>>>>>>>>>>");
			System.out.println("Subscribed to topic " + topicName);

			while (true) {
				ConsumerRecords<Integer, String> records = consumer.poll(100);
				for (ConsumerRecord<Integer, String> record : records) {
					System.out.println(">>>>>>>>>>>>Below are the records consumed>>>>>>>>>>>>");
					System.out.printf("offset = %d, key = %s, value = %s\n", 
							record.offset(), record.key(), record.value());
					// print the offset,key and value for the consumer records.
					this.collector.emit(new Values(record.value()));
					System.out.println(">>>>>>>>>>>>Emmited consumed value>>>>>>>>>>>>");
				}
			}
	}

	//Override all the interface methods
	public boolean isDistributed() {
		System.out.println(">>>>>>>>>>>>isDistributed method of Spout>>>>>>>>>>>>");
		return false;
	}

	public void deactivate() {
		System.out.println(">>>>>>>>>>>>deactivate method of Spout>>>>>>>>>>>>");
	}

	public void ack(Object msgId) {
		System.out.println(">>>>>>>>>>>>ack method of Spout>>>>>>>>>>>>");
	}

	public void fail(Object msgId) {
		System.out.println(">>>>>>>>>>>>fail method of Spout>>>>>>>>>>>>");
	}
	
	public void close() {
		System.out.println(">>>>>>>>>>>>close method of Spout>>>>>>>>>>>>");
	}
}