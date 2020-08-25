package com.lisz.hadoop.practice2;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoDaysOfHighestTemperatureSortComparator extends WritableComparator {

	public TwoDaysOfHighestTemperatureSortComparator() {
		super(TwoDaysOfHighestTemperatureKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TwoDaysOfHighestTemperatureKey k1 = (TwoDaysOfHighestTemperatureKey)a;
		TwoDaysOfHighestTemperatureKey k2 = (TwoDaysOfHighestTemperatureKey)b;
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
