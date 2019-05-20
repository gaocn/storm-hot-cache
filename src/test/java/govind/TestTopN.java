package govind;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.map.LRUMap;

import java.util.ArrayList;
import java.util.Map;

public class TestTopN {
//	public static void main(String[] args) {
//		LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);
//		productCountMap.put(2L, 44L);
//		productCountMap.put(1L, 33L);
//		productCountMap.put(3L, 55L);
//		productCountMap.put(6L, 88L);
//		productCountMap.put(4L, 66L);
//		productCountMap.put(8L, 100L);
//		productCountMap.put(7L, 99L);
//		productCountMap.put(5L, 77L);
//
//		int N = 3;
//		ArrayList<Map.Entry<Long, Long>> topN = new ArrayList<>();
//		for (Map.Entry<Long, Long> entry : productCountMap.entrySet()) {
//			if (topN.size() == 0) {
//				topN.add(entry);
//			} else {
//				int size = topN.size();
//				for (int i = 0; i < size; i++) {
//					if (entry.getValue() > topN.get(i).getValue()) {
//						if (size < N) {
//							topN.add(i, entry);
//						} else {
//							for (int j = size - 1; j > i ; j--) {
//								topN.set(j, topN.get(j - 1));
//							}
//							topN.set(i, entry);
//						}
//						break;
//					}
//					if (size < N) {
//						topN.add(entry);
//						break;
//					}
//				}
//			}
//		}
//		System.out.println(topN);
//
//	}
	public static void main(String[] args) {
		String json = "[{1:2},{3:4}]";
		JSONArray array = JSONArray.parseArray(json);
		System.out.println(array.getJSONObject(0).keySet());
		JSONObject jsonObject = JSONObject.parseObject(json);


	}
}
