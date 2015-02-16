package cn.com.diditaxi.hive.cf;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
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
        name = "extract_Date_from_sta",
        value = "_FUNC_(str) - 从edm日志中抽取Date",
        extended = "Example:\n" +
                "  > SELECT extract_Date_from_sta(sta) FROM file_test a;\n"
)
public final class ExtractEdmDate extends UDF {
	private Text r =  new Text();
    public static void main(String[] args) throws ParseException {
    	ExtractEdmDate ep = new ExtractEdmDate();
		String t = "Jan  4 16:29:58 mx174 postfix/smtp[4083]: connect to mx242.csdn.net[192.168.6.34]:25: Connection timed out";  	
    	System.out.println(ep.evaluate(t));
	}
    public Text evaluate(String sta) throws ParseException {
    	if (sta != null){
    		return parse(sta);
    	}else{
    		return null;
    	}
    }
	public Text parse(String status) throws ParseException {
		String[] result = status.split(" ");
		if(result.length > 1){
			String res = "";
			if(result[1].length() ==2){
				res = result[0]+" "+result[1];
			}
			else
			{
				res = result[0]+" "+result[2];
			}
			SimpleDateFormat sdf = new SimpleDateFormat("MMM dd", Locale.US);//MMM dd hh:mm:ss Z yyyy
			SimpleDateFormat sdf1 = new SimpleDateFormat("MM-dd", Locale.US);
	        try {
	            String date = sdf1.format(sdf.parse(res)).toString();
	            r.set(date);
	        } catch (ParseException ex) {
	            System.out.println("error");
	        }
        }
		return r;
	}
}
