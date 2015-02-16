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
public final class ConvertDate extends UDF {
	private Text r =  new Text();
    public static void main(String[] args) throws ParseException {
    	ConvertDate ep = new ConvertDate();
		Text t = new Text();  	
    	System.out.println(ep.evaluate(t));
	}
    public Text evaluate(Text text) throws ParseException {
    	
    	if (text != null){
    		String str = text.toString();
    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/M/d HH:mm:ss");//MMM dd hh:mm:ss Z yyyy
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        try {
	            String date = sdf1.format(sdf.parse(str)).toString();
	            r.set(date);
	        } catch (ParseException ex) {
	            System.out.println("error");
	        }
	        return r;
    	}else{
    		return null;
    	}
    }
}
