package govind.eshop.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<String> queue;
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		queue = new LinkedBlockingQueue<>();
		new Thread(new KafkaProcessor(queue)).start();
	}
	@Override
	public void nextTuple() {
		try {
			String message = queue.take();
			System.err.println("发射消息：" + message);
			collector.emit(new Values(message));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
	private static class KafkaProcessor implements Runnable {
		private KafkaConsumer consumer;
		private BlockingQueue<String> queue;
		public KafkaProcessor(BlockingQueue queue) {
			Properties props = new Properties();
			props.setProperty("bootstrap.servers", "192.168.211.128:9092,192.168.211.129:9092");
			props.setProperty("key.deserializer", StringDeserializer.class.getName());
			props.setProperty("value.deserializer", StringDeserializer.class.getName());
			props.setProperty("group.id", "access-log-consumer");
			props.setProperty("enable.auto.commit", "true");
			props.setProperty("auto.commit.interval.ms", "1000");
			this.queue = queue;
			this.consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList("AccessLog"));
		}
		@Override
		public void run() {
			while (true) {
				ConsumerRecords records = consumer.poll(100);
				Iterator<ConsumerRecord<String, String>> iter = records.iterator();
				while (iter.hasNext()) {
					try {

						queue.put(iter.next().value());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
