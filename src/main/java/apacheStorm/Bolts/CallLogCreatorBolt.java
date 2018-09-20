package apacheStorm.Bolts;

import java.net.URLClassLoader;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//Create a class CallLogCreatorBolt which implement IRichBolt interface
public class CallLogCreatorBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//Create instance for OutputCollector which collects and emits tuples to produce output
	private OutputCollector collector;
	private int componentId;
	private URLClassLoader urlClassLoader;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		System.out.println(">>>>>>>>>>>>BOLT2 Map method>>>>>>>>>>>>");
		this.collector = collector;
		this.componentId = context.getThisTaskId();
	}


	public void execute(Tuple tuple) {		
		String cartID = tuple.getStringByField("cartID");
		String customerID = tuple.getStringByField("customerID");
		String item = tuple.getStringByField("item");
		String quantity = tuple.getStringByField("quantity");
		String eventType = tuple.getStringByField("eventType");
		//Integer duration = tuple.getInteger(2);
		System.out.println(">>>>>>>>>>>>BOLT2 execute method field1>>>>>>>>>>>>"+ cartID +customerID + item +quantity+ eventType);
		collector.emit(new Values(cartID,customerID,item,quantity,eventType));
		collector.ack(tuple);
}


	public void cleanup() {
		System.out.println(">>>>>>>>>>>>BOLT2 cleanup method>>>>>>>>>>>>");
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println(">>>>>>>>>>>>BOLT2 declareOutputFields method>>>>>>>>>>>>");
		declarer.declare(new Fields("cartID","customerID","item","quantity","eventType"));
	}


	public Map<String, Object> getComponentConfiguration() {
		System.out.println(">>>>>>>>>>>>BOLT2 Map method>>>>>>>>>>>>");
		return null;
	}
}