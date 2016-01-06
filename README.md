# hadoop


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSequenceFile extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
//		job.setJarByClass(SmallFilesToSequenceFile.class);
		job.setJobName("smallfilestoseqfile");
		
		job.setMapperClass(SequenceFileMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(1);
		
	    return job.waitForCompletion(true) ? 0 : 1;
	
	}

	public static void main(String[] args) throws Exception {
	
		if(args.length<2){
			System.out.println("Enter input path and output dir path");
		}
		
		int n = ToolRunner.run(new SmallFilesToSequenceFile(), args);
		System.exit(n);
	}
}
