package com.lisz.hadoop.practice2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoDaysOfHighestTemperatureReducer extends Reducer<TwoDaysOfHighestTemperatureKey, IntWritable, Text, IntWritable> {
	private Text text = new Text();
	private IntWritable intWritable = new IntWritable();

	@Override
	protected void reduce(TwoDaysOfHighestTemperatureKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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

	private void writeResult(Context context, TwoDaysOfHighestTemperatureKey key) throws IOException, InterruptedException {
		text.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
		intWritable.set(key.getTemperature());
		context.write(text, intWritable);
	}
}
