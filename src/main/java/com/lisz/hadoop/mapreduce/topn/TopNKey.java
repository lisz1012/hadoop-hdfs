package com.lisz.hadoop.mapreduce.topn;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 自定义类型必须实现序列化反序列化和比较器接口
@Data
@AllArgsConstructor
@NoArgsConstructor // <-- 这里必须有这个无参构造，否则执行的时候反射会报错
public class TopNKey implements WritableComparable<TopNKey> {
	private int year;
	private int month;
	private int day;
	private int temperature;
	private String location;

	@Override
	public int compareTo(TopNKey that) {
		// 为了让这个案例体现API开发，所以下面的逻辑是一种通用的逻辑：按照时间的正序，
		// 但是我们目前业务需要的是年月温度，且温度倒序，所以一会儿还要开发一个sortComparator
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
		out.writeUTF(location);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		temperature = in.readInt();
		location = in.readUTF();
	}
}
