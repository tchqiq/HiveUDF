package cn.com.diditaxi.hive.cf;


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
        name = "extract_status_from_sta",
        value = "_FUNC_(str) - 从edm日志中抽取status",
        extended = "Example:\n" +
                "  > SELECT extract_status_from_sta(sta) FROM file_test a;\n"
)
public final class ExtractEdmFailCause extends UDF {
	public final static String regexpStr = "(status=(bounced|deferred)\\s)(\\(.*)";
	private Text r =  new Text();
    public static void main(String[] args) {
    	ExtractEdmFailCause ep = new ExtractEdmFailCause();
		Text t = new Text();
		t.set("Dec 30 16:11:48 mx174 postfix/smtp[13861]: 1517460211: to=<h438506624@12b6.com>, relay=smtp.secureserver.net[68.178.213.203]:25, delay=2.9, delays=0/0.01/1.5/1.4, dsn=5.1.1, status=bounced (host smtp.secureserver.net[68.178.213.203] said: 550 5.1.1 <h438506624@12b6.com> Recipient not found.  <http://x.co/irbounce> (in reply to RCPT TO command))");
		System.out.println(ep.evaluate(t));
	}
    
    public Text evaluate(Text sta) {
    	if (sta != null){
    		return parse(sta.toString());
    	}else{
    		return null;
    	}
    }

	public Text parse(String status) {
		String result = new String();
		String fileStr = status.toString();
        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (fileStr);
        if (m.find()) {
        	result = m.group(3);
        }
        r.set(result);
		return r;
	}
}
