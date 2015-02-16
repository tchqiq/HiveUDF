package cn.com.diditaxi.hive.cf;

//import is.tagomor.woothee.Classifier;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 9/14/13 leiyifei
 * 
 */
@Description(
        name = "extract_message_from_mes",
        value = "_FUNC_(str) - 从edm日志中抽取message_id",
        extended = "Example:\n" +
                "  > SELECT extract_message_from_mes(mes) FROM file_test a;\n"
)
public final class ExtractEdmMessageId extends UDF {
	public final static String regexpStr = "(message-id=)(\\d+\\.\\d+\\.\\d+\\.\\d+_\\d+_\\d+_([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})_\\d+)";
	private Map<String, String> r =  new HashMap<String, String>();
    public static void main(String[] args) {
    	ExtractEdmMessageId ep = new ExtractEdmMessageId();
		Text t = new Text();
		t.set("Dec 30 16:11:44 mx174 postfix/cleanup[13855]: 8DA2060220: message-id=<20141230081144.8DA2060220@mx174.csdn.net>");
		System.out.println(ep.evaluate(t));
	}
    public Map<String,String> evaluate(Text mess) {
    	if (mess != null){
    		return parse(mess.toString());
    	}else{
    		return null;
    	}
    }

	public Map<String,String> parse(String message) {
		String result = new String();
		String fileStr = message.toString();
        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (fileStr);
        if (m.find ()) {
        	result = m.group(2);
        }
		String[] arr = result.split("_");
		if(arr.length==5){
		r.put("ip",arr[0]);
		r.put("edm_id", arr[1]);
		r.put("task_id", arr[2]);
		r.put("email", arr[3]);
		r.put("logid", arr[4]);
		}
		return r;
	}
}
