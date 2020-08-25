package com.lisz.hadoop.practice2;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoDaysOfHighestTemperatureGroupingComparator extends WritableComparator {
	public TwoDaysOfHighestTemperatureGroupingComparator() {
		super(TwoDaysOfHighestTemperatureKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TwoDaysOfHighestTemperatureKey k1 = (TwoDaysOfHighestTemperatureKey)a;
		TwoDaysOfHighestTemperatureKey k2 = (TwoDaysOfHighestTemperatureKey)b;
		int c1 = Integer.compare(k1.getYear(), k2.getYear());
		if (c1 == 0) {
			return Integer.compare(k1.getMonth(), k2.getMonth());
		}
		return c1;
	}
}
