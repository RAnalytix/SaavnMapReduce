package com.upgrad.saavndemoproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


//Creating a composite key which is a combination of songid and the hour
//This is to ensure that the Reducer processes the data of a whole day based on 
//on the combination of songid and hour at hich song was played.
//The keys will not only be sorted based on single key but as a composite key


//This is a public class as this will be accessed by another class in the package
//We can create composite key by implementing WritableComparable interface which 
//make it use like any normal WritableComparable interface object

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {


	String song_id = "";
	int hour = 0;

	public void set(String songid , int hr) {
		this.song_id = songid;
		this.hour = hr;

	}

	public String getsongID() {
		return this.song_id;
	}

	public int getHour() {
		return this.hour;
	}

	//method from WritableComparable interface and hence needs to be implemented
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, song_id);
		WritableUtils.writeVInt(out, hour);
	}

	//method from WritableComparable interface and hence needs to be implemented
	@Override
	public void readFields(DataInput in) throws IOException {

		this.song_id = WritableUtils.readString(in);
		this.hour = WritableUtils.readVInt(in);
	}

	//method from WritableComparable interface and hence needs to be implemented
	//This method returns 0 / 1 / -1 based on how one composite key compares to another
	@Override
	public int compareTo(CompositeGroupKey other) {

		//The result is zero if the strings(songid) are equal , if not , return what is returned
		if(this.song_id.compareTo(other.song_id) != 0) {
			return this.song_id.compareTo(other.song_id);
		}

		//This compares integer value -> hour
		else if(this.hour != other.hour) {
			return (this.hour < other.hour ? -1 : 1) ;
		}
		else {
			return 0;
		}
	}

	//Returning the String which is concatenation of songid , : and hour's value in String
	@Override
	public String toString() {
		return song_id.toString() + ":" + String.valueOf(hour);
	}


	//This inner class cannot access non-static data members and methods. It can be accessed by outer class name
	public static class CompGroupKeyComparator extends WritableComparator {

		//The constructor of inner class
		public CompGroupKeyComparator() {
			super(CompositeGroupKey.class);
		}

	}

	
	//This block gets executed first when the class is loaded in the memory
	//It defines that this class - CompositeGroupKey should be used for comparing the keys
	//The map should not go with natural sort of key(songid) but with composite key sorting(songid + hour)
	
	static { // register this comparator
		WritableComparator.define(CompositeGroupKey.class,
				new CompGroupKeyComparator());
	}

}

