package me.huqiao.hadoop.demo_code.accesslog.responsetime;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class URLResponseTimeStarter {

	public static void main(String[] args)throws Exception {
		String inputPath = args[0];
        String outputPath = args[1];
        
        FileSystem fs = FileSystem.get(new URI("hdfs://vcentos1:9000"),new Configuration(),"root");
        
        //delete output path when it existed
        Path output = new Path(outputPath);
        if(fs.exists(output)) {
        	fs.delete(output,true);
        }
        
        Job job = Job.getInstance();
        
        job.setJarByClass(URLResponseTimeStarter.class);
        
        job.setMapperClass(URLResponseTimeMapper.class);
        job.setReducerClass(URLResponseTimeReducer.class);
        
        job.setMapOutputKeyClass(MyText.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(MyText.class);
        job.setOutputValueClass(LongWritable.class);
        
        
        job.setPartitionerClass(URLResponseTimePartitioner.class);
        //URLResponseTimePartitioner returns 1 or 0,so num of reduce task must be 2
        job.setNumReduceTasks(2);
        
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        boolean success = job.waitForCompletion(true);
        
        System.exit(success ? 0 : 1);
	}
	
}
