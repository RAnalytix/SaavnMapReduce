package com.upgrad.saavndemoproject;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//The job configuration object has a method - setOutputKeyComparatorClass, which takes only a Comparator as parameter
//Hence this class has been created to tell the JobConf to use this class to compare the composite key in the map phase

public class CompositeKeyComparator extends WritableComparator {
	
	protected CompositeKeyComparator() {
		super(CompositeGroupKey.class, true);
	}
	
	//This method compares 2 instances of WritableComparable interface , 
	//which in our case are the composite keys(songid and hour)
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		//The instances of interface are being casted into the class which has implemented this interface.
		//In our case - that class is - CompositeGroupKey
		
		CompositeGroupKey ip1 = (CompositeGroupKey) w1;
		CompositeGroupKey ip2 = (CompositeGroupKey) w2;

		//Both parts of the composite key instances(ip1 , ip2) are being compared to each other(songid-String , hour-int)
		int cmp = ip1.song_id.compareTo(ip2.song_id);
		if (cmp != 0) {
			return cmp;
		}

		return (ip1.hour == ip2.hour) ? 0 : ((ip1.hour < ip2.hour) ? -1 : 1);

	}


}
