package cn.com.diditaxi.hive.cf;


import java.io.IOException;
import java.security.MessageDigest;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/** 
 * 9/14/13 gengyf
 * 
 */
@Description(
        name = "extract_agent_from_agt",
        value = "_FUNC_(str) - 从agent中抽取预定义的浏览器类型",
        extended = "Example:\n" +
                "  > SELECT extract_agent_from_agt(agt) FROM file_pv_track a;\n"
)
public final class MD5 extends UDF {

    private Text output = new Text();
//    public static void main(String[] args) {
//    	MD5 ep = new MD5();
//		Text t = new Text();
//		t.set("http://www.csdn.net/");
//		System.out.println(ep.evaluate(t).toString());
//	}
    public Text evaluate(Text text) {
        output.set("");
        if (text == null) {
            return output;
        }
        String str = text.toString();
        String md5 = MD5(str);
        output.set(md5.toUpperCase());
        return output;
    }
    
    public final static String MD5(String s) {
		try {
			byte[] btInput = s.getBytes();
			MessageDigest mdInst = MessageDigest.getInstance("MD5");
			mdInst.update(btInput);
			byte[] md = mdInst.digest();
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < md.length; i++) {
				int val = ((int) md[i]) & 0xff;
				if (val < 16)
					sb.append("0");
				sb.append(Integer.toHexString(val));

			}
			return sb.toString();
		} catch (Exception e) {
			return null;
		}
	}

    
}
