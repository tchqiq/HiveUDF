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
public final class ExtractJobOrganId extends UDF {
	public final static String regexpStr = "(orgid=(.*))";
	private Text r =  new Text();
    public static void main(String[] args) {
    	ExtractJobOrganId ep = new ExtractJobOrganId();
		Text t = new Text();
		t.set("北京子衿晨风科技有限公司,http://pr.csdn.net/enterprise/ent_home?orgid=386661");
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
        	result = m.group(2);
        }
        r.set(result);
		return r;
	}
}
