package govind.eshop.bolt;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 访问日志解析，并发送productId给下一个bolt
 */
public class LogParseBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String message = input.getStringByField("message");
		JSONObject jsonObject = JSONObject.parseObject(message);
		JSONObject uriArgsJsonObj = jsonObject.getJSONObject("uri_args");
		Long productId = uriArgsJsonObj.getLong("productId");
		if (productId != null) {
			collector.emit(new Values(productId));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("productId"));
	}
}
