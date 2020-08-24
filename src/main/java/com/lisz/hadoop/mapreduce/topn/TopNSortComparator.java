package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNSortComparator extends WritableComparator {
	// 告诉比较器处理的类型是什么，byte数组读取的时候才好确定长度，见父类中另一个compare方法的实现
	public TopNSortComparator() {
		super(TopNKey.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		TopNKey k1 = (TopNKey)a;
		TopNKey k2 = (TopNKey)b;
		int c1 = Integer.compare(k1.getYear(), k2.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(k1.getMonth(), k2.getMonth());
			if (c2 == 0) {
				return Integer.compare(k2.getTemperature(), k1.getTemperature());
			}
			return c2;
		}
		return c1;
	}
}
