package apacheStorm.builder;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import apacheStorm.Bolts.CallLogCreatorBolt;
import apacheStorm.Bolts.CallLogCounterBolt;
import apacheStorm.Bolts.CallLogHbaseBolt;
import apacheStorm.spouts.FakeCallLogReaderSpout;


public class LogAnalyserStorm {
	public static void main(String[] args) throws Exception{
		Config config = new Config();
		config.setDebug(false);
		
		        
		//config.put("zookeeper.connect", "victoria.com:2181");
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

		builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt(),1)
		.shuffleGrouping("call-log-reader-spout");
			
		builder.setBolt("call-log-creater-bolt", new CallLogCreatorBolt(),1)
		.fieldsGrouping("call-log-counter-bolt", new Fields("cartID"));

		builder.setBolt("call-log-hbase-bolt", new CallLogHbaseBolt(),1)
		.fieldsGrouping("call-log-creater-bolt", new Fields("cartID"));
		
		//builder.setBolt("kafkaBolt", new KafkaBolt<String,Object>())
		//.shuffleGrouping("call-log-hbase-bolt");
		
		//builder.setBolt("call-log-kafka-bolt", new CallLogKafkaPublishBolt(),1)
		//.fieldsGrouping("call-log-hbase-bolt", new Fields("cartID"));
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
		System.out.println(">>>>>>>>>>>>Going to sleep>>>>>>>>>>>>");
		Thread.sleep(40000);
		System.out.println(">>>>>>>>>>>>Going to Kill topology>>>>>>>>>>>>");
		cluster.killTopology("LogAnalyserStorm");
		System.out.println(">>>>>>>>>>>>Going to Shutdown Cluster>>>>>>>>>>>>");
		cluster.shutdown();

	}

}
