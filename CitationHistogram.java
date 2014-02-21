/*
 * @author Amrut Patil
 * Project: Processing Patent Citation Data set.
 * This program counts the citation count in the input data to determine the distribution of counts.
 * Date: July 21, 2013
 * 
 */

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CitationHistogram{

	    public static class Map extends Mapper<Text, Text, IntWritable, IntWritable> {
	    	
	    	private final static IntWritable countone = new IntWritable(1);
        	private IntWritable citationCount = new IntWritable();
	    	
        	public void map(Text key, Text value,
	                        Context context) throws InterruptedException, IOException {
	    		citationCount.set(Integer.parseInt(value.toString()));
	            context.write(citationCount,countone);
	        }
	
	    }
    
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context) throws InterruptedException, IOException {
            int count = 0;
            //Counting the 1s for each citation count.
            for(IntWritable val:values){
            	count+=val.get();            	
            }   
            context.write(key,new IntWritable(count));
        }
	}
	
    public static void main(String[] args) throws Exception {
        
    	//Configuring a MapReduce job
    	Configuration conf = new Configuration();
 
    	//Object which stores configuration parameters for a job.
    	//Define and control execution of a job
        Job job = new Job(conf, "CitationHistogram");
        job.setJarByClass(CitationHistogram.class);
        
        //Takes care of file splitting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Set Mapper and Reducer
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //The way an input file should be split up and read by Hadoop.
        //Each record split in key/value pair separated by tab character(default)
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        
        //The way output should be written to files
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //Should be the same as type of <K2,V2>
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.waitForCompletion(true);
        
    }        
}

