package kick;

/*import java.io.File;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;*/
/*
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;*/

//import java.io.File;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// the keys are text (strings)
		job.setOutputKeyClass(NullWritable.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		// the values are text (strings)
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);
		job.setMapperClass(Map.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		Path outPath = new Path(args[1]);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}
	}
}
