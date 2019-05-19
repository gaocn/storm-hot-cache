package govind.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 单词计数拓扑
 */
public class WordCountTopology {
	/**
	 * Spout，继承基类接口，负责从从数据源获取数据。
	 */
	public static class RandomSentence extends BaseRichSpout {
		private SpoutOutputCollector collector;
		private Random rand;

		/**
		 * 对Spout进行初始化，例如创建线程池、数据库连接池、Http连接池等以
		 * 便获取数据
		 */
		@Override
		public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
			//open初始化时会传入SpoutOutputCollector是用来发射数据出去的。
			this.collector = spoutOutputCollector;
			//随机数生产对象
			this.rand = new Random();
		}

		/**
		 * 这段代码会放在某个worker进程的某个executor线程内的某个task中，
		 * 这个task会负责不断循环调用nextTuple不断发射最新数据出去从而形成
		 * 一个数据流。
		 */
		@Override
		public void nextTuple() {
			Utils.sleep(100);
			String[] sentences = new String[]{
					"the cow jumped over the moon",
					"an apple a day keeps the doctor away",
					"four score and seven years ago",
					"snow white and the seven dwarfs",
					"i am at two with nature"};
			final String sentence = sentences[rand.nextInt(sentences.length)];
			//Values用于构建一条tuple，tuple为最小数据单元
			System.err.println("发射句子：" + sentence);
			collector.emit(new Values(sentence));
		}

		/**
		 * 定义发射出去的每个tuple中的每个field的名称是什么
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sentence"));
		}
	}

	/**
	 * 负责拆分句子为若干个单词。
	 * 每个bolt的代码同样是发送到worker中的executor中task中运行
	 */
	public static class SplitBolt extends BaseRichBolt {
		private OutputCollector collector;

		@Override
		public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
			//Bolt的Tuple发射器
			this.collector = outputCollector;
		}

		/**
		 * 每次接收到一条数据后，就会交给executor方法来执行
		 */
		@Override
		public void execute(Tuple tuple) {
			String sentence = tuple.getStringByField("sentence");
			String[] splits = sentence.split(" ");
			for (int i = 0; i < splits.length; i++) {
				collector.emit(new Values(splits[i]));
			}
		}

		/**
		 * 定义发射出去的每个tuple中的每个field的名称是什么
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	/**
	 * 计数
	 */
	public static class CountBolt extends BaseRichBolt {
		private OutputCollector collector;
		private Map<String, Long> wordCount;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
			this.collector = outputCollector;
			this.wordCount = new HashMap<>();
		}

		@Override
		public void execute(Tuple tuple) {
			String word = tuple.getStringByField("word");
			Long count = wordCount.get(word);
			if (count == null) {
				count = 0L;
			}
			count++;
			wordCount.put(word, count);
			System.err.println("单词计数结果："+ word +"="+ count);
			collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
		//将spout、bolts组合起来构建一个拓扑
		TopologyBuilder builder = new TopologyBuilder();
		// spout名称，spout实例，spout的executor个数，默认一个executor中一个task
		builder.setSpout("RandomSentence", new RandomSentence(), 2);
		//每个executor中有20个task
		builder.setBolt("SplitSentence", new SplitBolt(), 5)
				.setNumTasks(10)
				.shuffleGrouping("RandomSentence");
		builder.setBolt("WordCount", new CountBolt(), 10)
				.setNumTasks(20)
				//相同单词从SplitSentence发射出来时需要发送到同一个task中
				.fieldsGrouping("SplitSentence", new Fields("word"));
		Config config = new Config();
		//命令行执行，打算提交到集群上去
		if (args != null && args.length > 0) {
			//配置启动多少个worker运行该任务
			config.setNumWorkers(3);
			//配置作业名称，配置对象，和拓扑
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		} else {
			//在本地模式运行
			config.setMaxTaskParallelism(3);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("WordCountTopology", config, builder.createTopology());

			Utils.sleep(10000);
			localCluster.shutdown();
		}
	}
}
