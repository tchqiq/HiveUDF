package cn.com.diditaxi.hive.cf;

import is.tagomor.woothee.Classifier;

import java.util.Map;

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
public final class ExtractAgent extends UDF {

    public static void main(String[] args) {
    	ExtractAgent ep = new ExtractAgent();
		Text t = new Text();
		t.set("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; GTB6; InfoPath.2; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 1.1.4322; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)");
		System.out.println(ep.evaluate(t));
	}
    
    public Map<String,String> evaluate(Text agt) {
    	if (agt != null){
    		return parse((agt.toString()));
    	}else{
    		return Classifier.parse(null);
    	}
    }

	public Map<String,String> parse(String agent) {
		Map r = Classifier.parse(agent);
		
		String customBrowse = null;
		if (agent.contains("Maxthon")){
			customBrowse = "遨游";
		}else if (agent.contains("TencentTraveler")) {
			customBrowse = "腾讯TT";
		}else if (agent.contains("The World")) {
			customBrowse = "世界之窗";
		}else if (agent.contains("SE 2.X MetaSr")) {
			customBrowse = "搜狗浏览器";
		}else if (agent.contains("360SE")) {
			customBrowse = "360安全浏览器";
		}else if (agent.contains("Avant Browser")) {
			customBrowse = "Avant";
		}else if (agent.contains("LBBROWSER")) {
			customBrowse = "猎豹浏览器";
		}else if (agent.contains("QIHU 360EE")) {
			customBrowse = "360极速浏览器";
		}else{
			customBrowse = "other";
		}
		
		if (customBrowse.equals("other")){
			if (r.get("name").equals("Internet Explorer")){
				String version = r.get("version").toString();
				
				String browse = "IE " + version.split("\\.")[0];
				r.put("name", browse);
			}
		}else {
			r.put("name", customBrowse);
		}
		return r;
	}
}
