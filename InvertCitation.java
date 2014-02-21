/*
 * @author Amrut Patil
 * Project: Processing Patent Citation Data set.
 * This program inverts the citation data in the input data.
 * Date: July 20, 2013
 * 
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertCitation{

	    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	        public void map(LongWritable key, Text value,
	                        Context context) throws InterruptedException, IOException {
	        	 String[] citation = value.toString().split(",");
	             context.write(new Text(citation[1]), new Text(citation[0]));
	        }
	    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws InterruptedException, IOException {
            String csv = "";
            for(Text val: values){
            	if(csv.length() > 0){
            		csv+=",";
            	}
            	csv+=val.toString();	
            }
            context.write(key, new Text(csv));     
        }
	}
	
    public static void main(String[] args) throws Exception {
        
    	//Configuring a MapReduce job
    	Configuration conf = new Configuration();
 
    	//Object which stores configuration parameters for a job.
    	//Define and control execution of a job
        Job job = new Job(conf, "InvertCitation");
        job.setJarByClass(InvertCitation.class);
        
        //Takes care of file splitting
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Set Mapper and Reducer
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //The way an input file should be split up and read by Hadoop
        job.setInputFormatClass(TextInputFormat.class);
        
        //The way output should be written to files
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //Should be the same as type of <K2,V2>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.waitForCompletion(true);
        
    }        
}