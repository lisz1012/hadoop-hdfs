package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TopNMapper extends Mapper<LongWritable, Text, TopNKey, IntWritable> {
	// map可能会被调用多次，定义在外边，减少GC，源码中看到了，map输出的key value是会被序列化进入buffer的
	private TopNKey topNKey = new TopNKey(0, 0, 0, 0);
	private IntWritable topNValue = new IntWritable();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	Calendar calendar = Calendar.getInstance();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 开发习惯：不要过于自信
		// value： 2019-6-1 22:22:22	1	39
		String s = value.toString();
		String strs[] = s.split("\\s+");
		String dateStr = strs[0];
//		String time = strs[1];
//		String code = strs[2];
		String temperature = strs[3];
		Date date = null;
		try {
			date = sdf.parse(dateStr);
			calendar.setTime(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		topNKey = new TopNKey(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), Integer.parseInt(temperature));
		topNValue.set(Integer.parseInt(temperature));
		context.write(topNKey, topNValue);
	}
}
