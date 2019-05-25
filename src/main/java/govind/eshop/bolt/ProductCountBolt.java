package govind.eshop.bolt;

import com.alibaba.fastjson.JSONArray;
import govind.eshop.ZookeeperSession;
import govind.eshop.http.HttpClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.Charsets;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 商品访问次数统计bolt
 */
public class ProductCountBolt extends BaseRichBolt {
	/**
	 * 使用LRU Map 统计商品访问次数，默认最多存放1000个常被访问的商品id
	 */
	private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);
	private int taskId;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.taskId = context.getThisTaskId();
		new Thread(new ProductCountThread()).start();
		new Thread(new HotProductFindThread()).start();
		initTaskid(context.getThisTaskId());
	}

	/**
	 * ProductCountBolt的所有Task在启动时，都会将自己的taskid写到同一个节
	 * 点的值中，格式：逗号拼接的列表。
	 * 并行运行并更新Znode中的内容，因此为了确保并发安全采用分布锁。
	 */
	private void initTaskid(int taskId) {
		ZookeeperSession session = ZookeeperSession.getInstance();
		session.acquireLock();
		String taskIdList = session.getZnodeData();
		if (!"".equals(taskIdList)) {
			taskIdList += "," + taskId;
		} else {
			taskIdList += taskId;
		}
		session.setZnodeData(taskIdList);
		session.releaseLock();
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
			List<Map.Entry<Long, Long>> topN = new ArrayList<>();
			int N = 3;
			while (true) {
				//每次都重新计算TopN
				topN.clear();
				if (productCountMap.size() == 0) {
					Utils.sleep(100);
					continue;
				}
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
									for (int j = size - 1; j > i; j--) {
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
				System.err.println("Top " + N + "商品搜索次数如下：" + topN);
				//获取TopN热门数据列表为字符串
				String topNProductList = JSONArray.toJSONString(topN);
				//幂等操作，每隔1分钟会将排名前三的信息发送到ZK中。
				ZookeeperSession.getInstance().setZnodeData("/task-hot-product-list-" + taskId, topNProductList);
				Utils.sleep(60 * 1000);
			}
		}
	}

	/**
	 * 热点数据统计
	 */
	private class HotProductFindThread implements Runnable {
		List<Map.Entry<Long, Long>> productCountList = new ArrayList<>();
		@Override
		public void run() {
			//1、将LRUMap中的数据按照访问次数进行全局排序
			List<Map.Entry<Long, Long>> orderedProds = new ArrayList<>();
			List<Long> hotProductIds = new ArrayList<>();
			List<Long> lastTimeHotProductIds = new ArrayList<>();
			int N = productCountMap.size();

			while (true) {
				//每次都重新计算TopN
				orderedProds.clear();
				hotProductIds.clear();

				if (productCountMap.size() == 0) {
					Utils.sleep(100);
					continue;
				}
				for (Map.Entry<Long, Long> entry : productCountMap.entrySet()) {
					if (orderedProds.size() == 0) {
						orderedProds.add(entry);
					} else {
						int size = orderedProds.size();
						for (int i = 0; i < size; i++) {
							if (entry.getValue() > orderedProds.get(i).getValue()) {
								if (size < N) {
									orderedProds.add(i, entry);
								} else {
									for (int j = size - 1; j > i; j--) {
										orderedProds.set(j, orderedProds.get(j - 1));
									}
									orderedProds.set(i, entry);
								}
								break;
							}
							if (size < N) {
								orderedProds.add(entry);
								break;
							}
						}
					}
				}
				System.err.println("排序后商品访问次数列表：" + orderedProds);
				//2、计算95%的商品访问次数的平均值
				int TP95 = (int) Math.ceil(orderedProds.size() * 0.95);
				long totalCount = 0L;
				for (int i = orderedProds.size() - 1; i >= orderedProds.size() - TP95 ; i--) {
					//从最少的次数统计平均次数
					totalCount += orderedProds.get(i).getValue();
				}
				//long avgCount = totalCount / orderedProds.size();
				long avgCount = 1;
				System.err.println("avgCount: " + avgCount);
				//3、遍历排序后商品访问次数，若某商品比平均访问次数多10倍，则认为是缓存热点。

				for (int i = 0; i < orderedProds.size(); i++) {
					if (orderedProds.get(i).getValue() > avgCount * 10) {
						Long productId = orderedProds.get(i).getKey();
						hotProductIds.add(productId);

						//4-1、将缓存热点对应的productId反向推送到流量分发的nginx本地缓存中
						String distributedUrl = "http://192.168.211.128:9080/hot?productId=" + productId;
						System.err.println(HttpClientUtils.sendGetRequest(distributedUrl));
						//4-2、获取缓存热点对应的完整商品信息，并反向推送到应用nginx的本地缓存中
						String cacheServiceUrl = "http://192.168.211.1:8080/getProductInfo/" + productId;
						String productInfo = HttpClientUtils.sendGetRequest(cacheServiceUrl);

						//对上述JSON字符串进行编码
						List<NameValuePair> params = new ArrayList<>();
						params.add(new BasicNameValuePair("productInfo", productInfo));
						productInfo = URLEncodedUtils.format(params, Charsets.UTF_8);

						String[] appNginxUrls = {
								"http://192.168.211.129:9080/hot?productId=" + productId + "&" + productInfo};
						for (String url : appNginxUrls) {
							System.err.println(HttpClientUtils.sendGetRequest(url));
						}
					}
				}
				//5. 实时感知热点数据的消失
				if (lastTimeHotProductIds.size() == 0) {
					if (hotProductIds.size() > 0) {
						lastTimeHotProductIds.addAll(hotProductIds);
					}
				} else {
					lastTimeHotProductIds.forEach(lastTimeId -> {
						if (!hotProductIds.contains(lastTimeId)) {
							//发送请求给流量分发nginx，取消热点缓存标识
							String url = "http://192.168.211.128:9080/cancelHot?productId=" + lastTimeId;
							System.err.println(HttpClientUtils.sendGetRequest(url));
						}
					});
					if (hotProductIds.size() > 0) {
						lastTimeHotProductIds.clear();
						lastTimeHotProductIds.addAll(hotProductIds);
					}
				}
				Utils.sleep(5000);
			}
		}
	}
}
