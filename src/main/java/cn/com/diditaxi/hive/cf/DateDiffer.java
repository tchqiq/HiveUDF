package cn.com.diditaxi.hive.cf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import com.google.common.collect.ImmutableMap;

/**
 * 12/05/14 hq
 */
@Description(
        name = "date_differ",
        value = "_FUNC_(startTime,endTime,format,type) - 给开始时间和结束时间返回时间差,单位天时分秒",
        extended = "Example:\n" +
                "  > SELECT date_differ(20140510 00:00:00,20140510 02:01:02,yyyyMMdd hh:mm:ss,s) \n" +
                "  FROM file_pv_track a;\n"
)
public final class DateDiffer extends UDF {

    private Long output = null;

    public static void main(String[] args) {
    	DateDiffer dd = new DateDiffer();
    	Text startTime = new Text("20140513 10:43:18");
    	Text endTime = new Text("20140513 12:43:18");
    	Text format = new Text("yyyyMMdd HH:mm:ss");
    	Text type = new Text("s");
    	System.out.println(dd.evaluate(startTime,endTime,format,type));
    	
	}
    public Long evaluate(Text startTime,Text endTime,Text format,Text type) {
        
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (format != null) {
        	sd = new SimpleDateFormat(format.toString());
        }
        
        if("-".equals(startTime.toString()) || "-".equals(endTime.toString())) {
        	return null;
        }
        
        output = dateDiff(startTime.toString(),endTime.toString(),
        		sd,type.toString());
        return output;
    }
    
    public static Long dateDiff(String startTime, String endTime,   
    		SimpleDateFormat sd, String type) {   
        final long nd = 1000 * 24 * 60 * 60;// 一天的毫秒数   
        final long nh = 1000 * 60 * 60;// 一小时的毫秒数   
        final long nm = 1000 * 60;// 一分钟的毫秒数   
        final long ns = 1000;// 一秒钟的毫秒数   
        long diff;   
        long day = 0;   
        long hour = 0;   
        long min = 0;   
        long sec = 0;  
        ImmutableMap<String, Long> imMap = null;
        try {   
        	// 获得两个时间的毫秒时间差异   
        	diff = sd.parse(endTime).getTime() - sd.parse(startTime).getTime();   
        	//diff = sd.parse(startTime).getTime() - sd.parse(endTime).getTime();   
            sec = diff/ns;
            min = diff/nm;
            hour = diff/nh;
            day = diff/nd;
            imMap = ImmutableMap.of("d", day,
				            		"h", hour, 
				            		"m", min, 
				            		"s", sec);
            
        } catch (ParseException e) {   
            e.printStackTrace();   
        }
		return imMap.get(type);   
    }
    
}
