package kick;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	File users=new File("hdfs://­localhost:54310/input/cooking1/users.xml");
	FolderHandler handler = new FolderHandler(); 

	/**
	 * Given an output filename, write a bunch of random records to it.
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws InterruptedException 
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException {
		try{
		SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
		parser.parse(users,handler);
		Text val=new Text("yo");
		if(value != null){
			context.write(val, handler.getValue());
		}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
