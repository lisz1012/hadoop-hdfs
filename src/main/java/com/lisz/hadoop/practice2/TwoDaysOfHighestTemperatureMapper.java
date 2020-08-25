package com.lisz.hadoop.practice2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TwoDaysOfHighestTemperatureMapper extends Mapper<LongWritable, Text, TwoDaysOfHighestTemperatureKey, IntWritable> {
	private TwoDaysOfHighestTemperatureKey twoDaysOfHighestTemperatureKey = new TwoDaysOfHighestTemperatureKey();
	private IntWritable intWritable = new IntWritable();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Calendar calendar = Calendar.getInstance();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 2019-6-1 22:22:22	1	38
		String strs[] = value.toString().split("\\s+");
		String dateStr = strs[0];
		String temperature = strs[3];
		try {
			calendar.setTime(sdf.parse(dateStr));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		twoDaysOfHighestTemperatureKey.setYear(calendar.get(Calendar.YEAR));
		twoDaysOfHighestTemperatureKey.setMonth(calendar.get(Calendar.MONTH) + 1);
		twoDaysOfHighestTemperatureKey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
		twoDaysOfHighestTemperatureKey.setTemperature(Integer.parseInt(temperature));
		intWritable.set(Integer.parseInt(temperature));
		context.write(twoDaysOfHighestTemperatureKey, intWritable);
	}
}
