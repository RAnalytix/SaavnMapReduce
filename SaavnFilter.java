package com.upgrad.saavndemoproject;

//All the required packages are added here

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;

public class SaavnFilter extends Configured implements Tool
{

	//Once we define composite key we create the mapper class which uses input generated from InputFormat
	//Extend Mapper class to basic generic Mapper<Key1, Value1, Key2, Value2> class 
	//which indicate the input & output for key and value(s)
	//Output in our case is - CompositeGroupKey and DateAndCount
	
	public static class MapperClass extends MapReduceBase implements Mapper<LongWritable, Text, CompositeGroupKey, DateAndCount> 
	{
		//Creating a list of days, as the date column 
		public static List<String> days = Arrays.asList("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31");	

		@Override
		public void map(LongWritable inkey, Text record, 
				OutputCollector<CompositeGroupKey, DateAndCount> output , Reporter reporter) throws IOException
		{

			//Checking if the record is not null, then only start processing it
			if(record != null) {
				
				//Split the record based on "," , all columns are separated by :
				String[] info = record.toString().split(",");

				//Checking if the array that holds different column values is not null and has length ==5
				//This is to ward off any data anomalies
				if( info != null && info.length == 5 ) {
					String songid = info[0];

					try {

						int hour = Integer.parseInt(info[3]);
						
						//This is checking if the column value which is held by variable - hour is between 0 and 23 only
						//This is again to ward off any data anomalies in hour column
						
						if(hour >=0 && hour <=23) {
							
							//Creating the compositegroupKey instance.
							CompositeGroupKey compositeGrpKey = new CompositeGroupKey();
							compositeGrpKey.set(songid, hour);
							
							//The day value is being extracted from the 5th column - which is timestamp
							String[] date = info[4].split("-");
							String day;
							
							//Checking if the date column contains some data
							if(date.length>1){
								day = date[2];

								if(days.contains(day)){
									
									//The output from Mapper will be <key , value> -> composite key and 
									//DateAndCount instance as value
									
									output.collect(compositeGrpKey , new DateAndCount(day,1));
								}
							}

						}
					}catch(Exception e) {
						
						//If any exception caught in Mapper, exception will be caught neatly
						e.printStackTrace();
					}

				}

			}//If the record is null, execute below code
			else {

				reporter.notify();
			}
		}
	}
	public static class DayPartitioner extends MapReduceBase implements Partitioner<CompositeGroupKey, DateAndCount>, Configurable {

		//introduced the partitioner to break the contract that all values corresponding to a key will go to 1 reducer
		//instead we want all songids,hour [composite key] in a single day reach a reducer
		
		private Configuration configuration;
		
		//Hash map has been created to hold each day's key value pair
		HashMap<String, Integer> daymap = new HashMap<String, Integer>();

		// Set up the months hash map in the setConf method.
		public void setConf(Configuration configuration) {
			this.configuration = configuration;
			daymap.put("01", 0);
			daymap.put("02", 1);
			daymap.put("03", 2);
			daymap.put("04", 3);
			daymap.put("05", 4);
			daymap.put("06", 5);
			daymap.put("07", 6);
			daymap.put("08", 7);
			daymap.put("09", 8);
			daymap.put("10", 9);
			daymap.put("11", 10);
			daymap.put("12", 11);
			daymap.put("13", 12);
			daymap.put("14", 13);
			daymap.put("15", 14);
			daymap.put("16", 15);
			daymap.put("17", 16);
			daymap.put("18", 17);
			daymap.put("19", 18);
			daymap.put("20", 19);
			daymap.put("21", 20);
			daymap.put("22", 21);
			daymap.put("23", 22);
			daymap.put("24", 23);
			daymap.put("25", 24);
			daymap.put("26", 25);
			daymap.put("27", 26);
			daymap.put("28", 27);
			daymap.put("29", 28);
			daymap.put("30", 29);
			daymap.put("31", 30);

		}


		// Implement the getConf method for the Configurable interface.

		public Configuration getConf() {
			return configuration;
		}

		//The getPartition method is going to return an int value based on the daymap hash map created
		//Each integer value is going to be between 0 and 30.
		//Now instead of sending all values for a (songid+hour) composite key to 1 reducer,
		//The partitioner is going to send them to multiple reducers based on day of the month.
		
		public int getPartition(CompositeGroupKey key, DateAndCount value, int numReduceTasks) {
			return (int) (daymap.get(value.getDate()));
		}


	}

	//Extend Reducer class to basic generic Reducer<Key1, Value1, Key2, Value2> class
	//Reducer processes collection of values for each key and write to the disk
	
	public static class ReducerClass extends MapReduceBase implements Reducer<CompositeGroupKey, DateAndCount, Text, Text> 
	{

		//The reduce method takes the Compositekey coming from Map and iterates over the list of values.
		//It finally emits key and value pairs which have (songid+hour) as key and sum of songs played for this combination
		
		public void reduce(CompositeGroupKey key, Iterator<DateAndCount> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			int song_count=0;
			int key_hour = 0;

			String key_out = key.getsongID() + '\t' + key.getHour();
			key_hour = key.getHour();

			//This keeps on summing up the number of times a certain song id was played in a certain hour
			while (values.hasNext()) {
				song_count = song_count + values.next().getCount();
			}
			
			//The logic is to add weights to the song count if the song has have gathered 
			//relatively high numbers of streams within small time windows (e.g. the last four hours)
			//Based on the different hours of the day, assign weights linearly
			//The songs in recent past will get more weight to calculate trending list for next day
			//The songs played between 12 - 4 will have more weightage because a lot of people play music while working
			
			//Only if a certain song has gathered more than 5 streams in any hour, it should be counted for assigning weight
			
			if(song_count > 5) {
				if(key_hour >=4  && key_hour <=7) {

					song_count = song_count*2;
				}
				else if(key_hour >=8  && key_hour <=11) {
					song_count = song_count*3;
				}
				else if(key_hour >=12  && key_hour <=15) {
					song_count = song_count*6;
				}
				else if(key_hour >=16  && key_hour <=19) {

					song_count = song_count*5;
				}
				else if(key_hour >=20  && key_hour <=23) {

					song_count = song_count*4;
				}
				else {

					song_count = song_count*1;
				}
			}

			String out = ":" +song_count;
			
			//It finally emits key and value pairs which have (songid+hour) as key and sum of songs played for this combination
			output.collect(new Text(key_out), new Text(out));

		}


	}


	//This main method calls the MapReduce Job. 
	//Before calling the job we need to set the MapperClass, ReducerClass, OutputKeyClass and OutputValueClass.
	public static void main(String[] args) throws Exception  {

		
		//JobConf is the main configuration class, which configure MapReduce parameters 
		//such as Mapper, Reducer, Combiner, InputFormat, OutputFormat, and Comparator
		
		JobConf conf = new JobConf(SaavnFilter.class);
		conf.setJobName("SaavnTrendingSongs");

		conf.setMapperClass(MapperClass.class);
		conf.setReducerClass(ReducerClass.class);

		conf.setMapOutputKeyClass(CompositeGroupKey.class);
		conf.setMapOutputValueClass(DateAndCount.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setOutputKeyComparatorClass(CompositeKeyComparator.class);

		conf.setNumReduceTasks(31);

		/*
		 * Specify the partitioner class.
		 */
		conf.setPartitionerClass(DayPartitioner.class);
		JobClient.runJob(conf);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}


}
