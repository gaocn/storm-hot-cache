package govind.eshop.bolt;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 商品访问次数统计bolt
 */
public class ProductCountBolt extends BaseRichBolt {
	/**
	 * 使用LRU Map 统计商品访问次数，默认最多存放1000个常被访问的商品id
	 */
	private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		new Thread(new ProductCountThread()).start();
	}

	@Override
	public void execute(Tuple input) {
		Long productId = input.getLongByField("productId");
		Long count = productCountMap.get(productId);
		if (count == null) {
			count = 0L;
		}
		count++;
		productCountMap.put(productId, count);
		System.err.println("产品productId=" + productId + " 访问次数：" + count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
	/**
	 * 后台线程，每隔1分钟计算出TopN个热门的商品id
	 */
	private class ProductCountThread implements Runnable {
		@Override
		public void run() {
			//TopN算法实现，基于大根堆小根堆实现算法O(NlogK)，这里不借助额外
			// 的数据结构实现，而采用最简单的O(N^2)算法
			List<Map.Entry<Long,Long>> topN = new ArrayList<>();
			int N = 3;
			while (true) {
				//每次都重新计算TopN
				topN.clear();
				for (Map.Entry<Long, Long> entry : productCountMap.entrySet()) {
					if (topN.size() == 0) {
						topN.add(entry);
					} else {
						int size = topN.size();
						for (int i = 0; i < size; i++) {
							if (entry.getValue() > topN.get(i).getValue()) {
								if (size < N) {
									topN.add(i, entry);
								} else {
									for (int j = size - 1; j > i ; j--) {
										topN.set(j, topN.get(j - 1));
									}
									topN.set(i, entry);
								}
								break;
							}
							if (size < N) {
								topN.add(entry);
								break;
							}
						}
					}
				}
				System.err.println("Top " + N +"商品搜索次数如下：" + topN);
				Utils.sleep(60 * 1000);
			}
		}
	}
}
