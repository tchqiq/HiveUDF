package cn.com.diditaxi.hive.cf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 1/26/14 WilliamZhu(allwefantasy@gmail.com)
 */
@Description(
        name = "u_json",
        value = "_FUNC_(str,key) - 从json格式数据中获取Key,默认为-",
        extended = "Example:\n" +
                "  > SELECT ParseJson('data','pid') FROM file_pv_track a;\n"
)
public class ExtractFileType extends UDF {
    private Text output = new Text("-");
    public final static String regexpStr = "\\.(doc|xls|ppt|avi|txt|gif|jpg|jpeg|bmp|png|zip|rar|swf|pdf|js|css|php|asc|psd|3ds|wmf|pcx|psp|mpeg|mpg|qtm|wav|html|htm|asp|dhtml|cgi|jar|exe|bat|dll|bak|ini|log|ico)";
    
    

    public Text evaluate(Text str) {
        output.set("-");
        if (str == null) {
            return output;
        }
        
        String fileStr = str.toString();
        Pattern p = Pattern.compile (regexpStr);
        Matcher m = p.matcher (fileStr);
        
        if (m.find ()) {
        	output.set(m.group(1));
        }
        
        
        return output;
    }
    
    public static void main(String[] args) {
		ExtractFileType eft = new ExtractFileType();
		System.out.println(eft.evaluate(new Text("GET /nginx_status HTTP/1.1")));
		
	}

}
