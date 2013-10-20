package kick;

import org.apache.hadoop.io.Text;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class FolderHandler extends DefaultHandler {
	 Text value=null;
	 public Text getValue() {
		return value;
	}
	public void setValue(Text value) {
		this.value = value;
	}
	public void startElement (String uri, String localName,String qName, Attributes attributes) throws SAXException
	    {
		 	System.out.println(attributes.getValue("OwnerUserId") +" " + attributes.getValue("ParentId"));
	    	if(attributes.getValue("PostTypeId").equals("2")){
	    		Text keyWords =new Text(attributes.getValue("OwnerUserId"));
				Text valueWords = new Text(attributes.getValue("ParentId"));
				String val=keyWords.toString()+" "+valueWords.toString();
				// Write the sentence 
				//System.out.println(keyWords.toString() + " :: " + val);
				if(keyWords != null && valueWords != null){
					setValue(new Text(val));
				}
	    	}
	    }

}
