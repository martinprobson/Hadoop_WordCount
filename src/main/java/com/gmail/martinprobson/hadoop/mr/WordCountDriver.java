package com.gmail.martinprobson.hadoop.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gmail.martinprobson.hadoop.util.HDFSUtil;

/**
 * Driver class for the Word Count Example.
 * 
 * <h4>Summary</h4>
 * <p>This class implements the standard Hadoop Tool interface and expects to be passed 
 * two arguments specifying the input and output directories as follows:
 * <p>
 * Running on a cluster via yarn: -
 * <p>
 * <code>
 * yarn jar jar_name com.gmail.martinprobson.hadoop.mr.WordCountDriver input_dir output_dir
 * </code>
 * <p>Running locally: -
 * <p>
 * <code>
 * hadoop --config local_config_file com.gmail.martinprobson.hadoop.mr.WordCountDriver input_dir output_dir
 * </code>
 * <p>Both run configurations are setup in the project pom.xml as goals <code>exec:exec@run-cluster</code> and 
 * <code>exec:exec@run-local</code> respectively.<p>
 * 
 * @author martinr
 *
 */
public class WordCountDriver extends Configured implements Tool {

    @SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(HDFSUtil.class);
	
    /**
     * Execute a Word Count Map Reduce job.
     * 
     * @param args input directory, output directory.
     * @return exit code.
     * @throws Exception
     */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic_options] <input path> <output path>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Job job = Job.getInstance(getConf());
		job.setJarByClass(WordCountDriver.class);
		job.setJobName("Hadoop MR Word Count");
		// Ensure that all files are read in the directory
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Check if output path exists and delete it if it does
		Path outputPath = new Path(args[1]);
		if (HDFSUtil.pathExists(getConf(),outputPath))
			HDFSUtil.deletePath(getConf(),outputPath);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

    /**
     * Execute a Word Count Map Reduce job via command line.
     * 
     * @param args input directory, output directory.
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountDriver(),args);
		System.exit(exitCode);
	}
}