package govind.eshop;

import govind.eshop.bolt.LogParseBolt;
import govind.eshop.bolt.ProductCountBolt;
import govind.eshop.spout.KafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 热数据统计拓扑
 */
public class HopProductTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(), 1);
		builder.setBolt("LogParseBolt", new LogParseBolt(), 2)
				.setNumTasks(4)
				.shuffleGrouping("KafkaSpout");
		builder.setBolt("ProductCountBolt", new ProductCountBolt(), 2)
				.setNumTasks(4)
				.fieldsGrouping("LogParseBolt", new Fields("productId"));

		Config config = new Config();
		config.setNumWorkers(4);

		if (args != null && args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("HopProductTopology", config, builder.createTopology());
			Utils.sleep(30 * 60 * 1000);
			localCluster.shutdown();
		}
	}
}
