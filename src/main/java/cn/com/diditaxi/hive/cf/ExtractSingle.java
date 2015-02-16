package cn.com.diditaxi.hive.cf;


import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/** 
 * 9/14/13 gengyf
 * 
 */
@Description(
        name = "extract_single",
        value = "_FUNC_(str) - 将一条数据按指定分隔符分隔并取分隔出的第一条数据",
        extended = "Example:\n" +
                "  > SELECT extract_single('java,java,java',',') FROM file_pv_track ;\n"
)
public final class ExtractSingle extends UDF {

    private Text output = new Text();
//    public static void main(String[] args) {
//    	ExtractSingle ep = new ExtractSingle();
//		Text t = new Text();
//		Text t2 = new Text();
//		t.set("java,java,java,java");
//		t2.set(",");
//		System.out.println(ep.evaluate(t,t2).toString());
//	}
    public Text evaluate(Text field, Text separator) {
        output.set("");
        if (field == null) {
            return output;
        }
        String s = ",";
        Set<String> set = new HashSet<String>();
        if (separator != null){
        	s = separator.toString();
        }
        for (String str : field.toString().split(s)){
        	set.add(str);
        }
        if (set.size() == 1){
        	output.set(field.toString().split(s)[0]);
        }else {
        	output.set(field);
		}
        return output;
    }
}
