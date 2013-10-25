import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;



public class Recommend {


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Recommend");
		job.setJarByClass(Recommend.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// the keys are text (strings)
		job.setOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// the values are text (strings)
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);
		job.setMapperClass(Map.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
