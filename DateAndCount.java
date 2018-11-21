package com.upgrad.saavndemoproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

//This class implements Writable interface
public class DateAndCount implements Writable {
	String date;
	int count;

	public DateAndCount() {super();}

	//The constructor for initializing the class variables
	public DateAndCount(String date, int count) {
	    this.date = date;
	    this.count = count;
	}

	//Getter and Setter methods for the class variables
	public String getDate() {return date;}
	public void setDate(String number) {this.date = number;}
	public int getCount() {return count;}
	public void setCount(int count) {this.count = count;}

	//The readFields method is defined how to read the data from Input
	public void readFields(DataInput dataInput) throws IOException {
	    date = WritableUtils.readString(dataInput);
	    count = WritableUtils.readVInt(dataInput);      
	}

	//The write method is defined to write the output
	public void write(DataOutput dataOutput) throws IOException {
	    WritableUtils.writeString(dataOutput, date);
	    WritableUtils.writeVInt(dataOutput, count);

	}

}
