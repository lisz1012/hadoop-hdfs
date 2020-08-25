package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNReducer extends Reducer<TopNKey, IntWritable, Text, IntWritable> {
	private Text text = new Text();
	private IntWritable intWritable = new IntWritable();
	/*
	这里的参数values是通过org.apache.hadoop.mapreduce.task.ReduceContextImpl 中的getValues()方法得到的调用
	org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer.getValues()得到的（WrappedReducer中的reduceContext是一个
	ReduceContextImpl 类的对象）。而这个在Reducer.run中返回的values是一个ValueIterable类型的对象，这个对象里面有一个ValueIterator
	类型的对象，而后者的next()方法里面调用了getNextKeyValue(), 而在getNextKeyValue()方法中
	有这么一句：key = keyDeserializer.deserialize(key);改变了key的引用所指向的对象，而在父类Reducer的run中，会调用
	reduce(context.getCurrentKey(), context.getValues(), context); 其中context.getCurrentKey()会把key返回
	ReduceContextImpl中的
	while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    是为了在Reducer.run中调用nextKey()的时候，通过 nextKeyIsSame为true，迅速跳到下一个而不做任何事情空转，什么时候!hasMore或者
    !nextKeyIsSame了，才返回true（或者false），使得继续执行。第一组的reduce方法返回之后，有可能空转，然后再进入第二组的reduce方法。
    相当于是在外层的while循环的循环条件方法里放空，然后到该读的地方继续读取
	 */
	@Override
	protected void reduce(TopNKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Integer firstDay = null;
		for (IntWritable i : values) {
			if (firstDay != null && key.getDay() != firstDay) {
				writeResult(context, key);
				return;
			}
			if (firstDay == null) {
				firstDay = key.getDay();
				writeResult(context, key);
			}
		}
	}

	private void writeResult(Context context, TopNKey key) throws IOException, InterruptedException {
		text.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "-" + key.getLocation());
		intWritable.set(key.getTemperature());
		context.write(text, intWritable);
	}
}
