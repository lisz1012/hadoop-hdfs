package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNGroupingComparator extends WritableComparator {

	// 告诉比较器处理的类型是什么，byte数组读取的时候才好确定长度，见父类中另一个compare方法的实现
	public TopNGroupingComparator() {
		super(TopNKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TopNKey k1 = (TopNKey)a;
		TopNKey k2 = (TopNKey)b;
		int c1 = Integer.compare(k1.getYear(), k2.getYear());
		if (c1 == 0) {
			return Integer.compare(k1.getMonth(), k2.getMonth());
		}
		return c1;
	}
}
