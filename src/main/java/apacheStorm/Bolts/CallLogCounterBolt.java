package apacheStorm.Bolts;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CallLogCounterBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	Map<String, Integer> counterMap;
	private OutputCollector collector;
	private int componentId;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		System.out.println(">>>>>>>>>>>>BOLT prepare method>>>>>>>>>>>>");
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
		this.componentId = context.getThisTaskId();
	}


	public void execute(Tuple tuple) {
		String call = tuple.getString(0);
		System.out.println(this.componentId+"Bolt execute method>>>>>>>>>>>>"+call);
		//String call2 = tuple.getString(1);		
		//System.out.println(this.componentId+">>>>>>>>>>>>Call2"+call2+">>>>>>>>>>>>");
		
		//String[] arr = call.split(" ");
		//for (String s : arr) {
		//      //collector.emit(new Values(s, 1));
		//	  collector.emit(new Values(s));
		//      System.out.println(">>>>>>>>>>>>BOLT execute collector.emit>>>>>>>>>>>>" + s);
		//    }
		if (call != null) {
			String[] cartArr = call.split(",");
			collector.emit(new Values(cartArr[0], cartArr[1], cartArr[2], cartArr[3], cartArr[4]));
			System.out.println("--------------》" + cartArr[0] + cartArr[1]+ cartArr[2]+ cartArr[3]+ cartArr[4]);
		}
		collector.ack(tuple);
	}


	public void cleanup() {
		System.out.println(">>>>>>>>>>>>BOLT cleanup method>>>>>>>>>>>>");
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println(">>>>>>>>>>>>BOLT declareOutputFields method>>>>>>>>>>>>");
		declarer.declare(new Fields("cartID","customerID","item","quantity","eventType"));
	}


	public Map<String, Object> getComponentConfiguration() {
		System.out.println(">>>>>>>>>>>>BOLT Map method>>>>>>>>>>>>");
		return null;
	}

}