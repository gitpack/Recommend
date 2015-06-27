/**
 * @author ShokeenAnil
 * This is the mapper file for the Recommendation engine for stackoverflow.
 * 
 * Written for a single system but can easily be configured on a cluster.
 * 
 * */
import java.io.IOException;
import java.util.StringTokenizer;

import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException {
		StringTokenizer tok= new StringTokenizer(value.toString());

		String pa=null,ow=null,pi=null;
		while (tok.hasMoreTokens()) {
			String[] arr;
			String val = (String) tok.nextToken();
			if(val.contains("PostTypeId")){
				arr= val.split("[\"]");
				pi=arr[arr.length-1];
				if(pi.equals("2")){
					continue;
				}
				else break;
			}
			if(val.contains("ParentId")){
				arr= val.split("[\"]");
				pa=arr[arr.length-1];
			} 
			else if(val.contains("OwnerUserId") ){
				arr= val.split("[\"]");
				ow=arr[arr.length-1];
				try {
					if(pa!=null && ow != null)
						context.write(new Text(ow),new Text(pa));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


	}

}


