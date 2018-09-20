package apacheStorm.Bolts;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes; 
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

//Create a class CallLogCreatorBolt which implement IRichBolt interface
public class CallLogHbaseBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	//Create instance for OutputCollector which collects and emits tuples to produce output
	private OutputCollector collector;
	private Connection connection;
	private Table table;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		System.out.println(">>>>>>>>>>>>BOLT3 Map method>>>>>>>>>>>>");
			Configuration hbaseconfig = HBaseConfiguration.create();     
			hbaseconfig.set("hbase.zookeeper.quorum", "victoria.com");
			hbaseconfig.set("hbase.zookeeper.property.clientPort", "2181");        
			hbaseconfig.set("zookeeper.znode.parent", "/hbase-unsecure");
			try {
				connection = ConnectionFactory.createConnection(hbaseconfig);
				table = connection.getTable(TableName.valueOf("ecomapp"));
			} catch (IOException e) {
	            //do something to handle exception
			}
		/*try {
		      HTable table = new HTable(hbaseconfig, "kafkaMessages");
		      // Instantiating Get class
		      Get g = new Get(Bytes.toBytes("dual"));
		      // Reading the data
		      Result result = table.get(g);
		      // Reading values from Result class object
		      byte [] value = result.getValue(Bytes.toBytes("image_meta"),Bytes.toBytes("X"));
		      // Printing the values
		      String name = Bytes.toString(value);
			System.out.println(">>>>>>>>>>>>BOLT3 HBASE table value>>>>>>>>>>>>" + name);
		}catch(IOException e) {
			//do something to handle exception
			System.out.println(">>>>>>>>>>>>BOLT3 HBASE table not found>>>>>>>>>>>>");
		}*/	
	}

	public void execute(Tuple tuple) {		
		String cartID = tuple.getStringByField("cartID");
		String customerID = tuple.getStringByField("customerID");
		String item = tuple.getStringByField("item");
		String quantity = tuple.getStringByField("quantity");
		String eventType = tuple.getStringByField("eventType");		
		//Integer duration = tuple.getInteger(2);
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String date = df.format(new Date());
		Calendar cal = Calendar.getInstance();
		cal = Calendar.getInstance();
		cal.add(Calendar.HOUR,-4);
		String dateMinusFourHour = df.format(cal.getTime());
		System.out.println(">>>>>>>>>>>Four hours to current date : " + dateMinusFourHour);
		try {
			Put put = new Put(Bytes.toBytes(cartID.trim()+customerID.trim()+item.trim()+quantity.trim()+eventType.trim()+date));//Adding rowID
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("composite_key"), Bytes.toBytes(customerID.trim()+item.trim()+eventType.trim()));
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("cartID"), Bytes.toBytes(cartID));
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("customerID"), Bytes.toBytes(customerID));
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("item"), Bytes.toBytes(item));
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("quantity"), Bytes.toBytes(quantity));
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("eventType"), Bytes.toBytes(eventType));
			put.addColumn(Bytes.toBytes("cartinfo"), Bytes.toBytes("date"), Bytes.toBytes(date));
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
			Scan scan = new Scan();
			//scan.addFamily(Bytes.toBytes("cartinfo"));
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter singleColumnValueFilter = new 
					SingleColumnValueFilter(Bytes.toBytes("cartinfo"), Bytes.toBytes("date"),  
	                CompareOp.LESS_OR_EQUAL,Bytes.toBytes(dateMinusFourHour));
			singleColumnValueFilter.setFilterIfMissing(true);
			SingleColumnValueFilter singleColumnValueFilter2 = new 
					SingleColumnValueFilter(Bytes.toBytes("cartinfo"), Bytes.toBytes("composite_key"),  
	                CompareOp.EQUAL,Bytes.toBytes(customerID.trim()+item.trim()+eventType.trim()));
			singleColumnValueFilter2.setFilterIfMissing(true);
			filterList.addFilter(singleColumnValueFilter);
			filterList.addFilter(singleColumnValueFilter2);
			scan.setFilter(filterList);
			System.out.println(">>>>>>>>>>begin scan >>>>>>>>>>");
	
			ResultScanner ss = null;
			try {
				ss = table.getScanner(scan);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (Result r : ss) {
				Cell[] rawCells = r.rawCells();
				String cartFound = "";			
				for (int i = 0; i < rawCells.length; i++) {
					Cell cell = rawCells[i];
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					cartFound = cartFound + qualifier + ":" + value + ", ";
				}
				System.out.println(">>>>>>>>>>Result Found In Database : "+cartFound);
			}
			System.out.println(">>>>>>>>>>scan end >>>>>>>>>>");
			
			System.out.println(">>>>>>>>>>>>BOLT3 execute method field1>>>>>>>>>>>>"+ cartID +customerID + item + eventType + quantity + date);
			collector.emit(new Values(cartID,customerID,item,eventType));
			collector.ack(tuple);
}

	public void cleanup() {
		System.out.println(">>>>>>>>>>>>BOLT3 cleanup method>>>>>>>>>>>>");
        try {
            if(table != null) table.close();
        } catch (Exception e){
            //do something to handle exception
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                //do something to handle exception
            }
        }
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println(">>>>>>>>>>>>BOLT3 declareOutputFields method>>>>>>>>>>>>");
		declarer.declare(new Fields("cartID","customerID"));
	}

	public Map<String, Object> getComponentConfiguration() {
		System.out.println(">>>>>>>>>>>>BOLT3 Map method>>>>>>>>>>>>");
		return null;
	}
}