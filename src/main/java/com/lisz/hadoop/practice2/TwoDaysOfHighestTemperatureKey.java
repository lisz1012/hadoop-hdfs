package com.lisz.hadoop.practice2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TwoDaysOfHighestTemperatureKey implements WritableComparable<TwoDaysOfHighestTemperatureKey> {
	private int year;
	private int month;
	private int day;
	private int temperature;

	@Override
	public int compareTo(TwoDaysOfHighestTemperatureKey that) {
		int c1 = Integer.compare(year, that.year);
		if (c1 == 0) {
			int c2 = Integer.compare(month, that.month);
			if (c2 == 0) {
				return Integer.compare(day, that.day);
			}
			return c2;
		}
		return c1;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(temperature);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		temperature = in.readInt();
	}
}
