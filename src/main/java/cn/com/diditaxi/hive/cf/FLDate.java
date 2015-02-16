package cn.com.diditaxi.hive.cf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "FLDate",
			value = "_FUNC_(from,period,fl) - 得到周或月的的第一天或最后一天\n"
					+"period:week|month;fl:f|l", 
			extended = "Example:\n"
						+ "  > SELECT FLDate(from,'period','f') FROM file_pv_track a;\n")
public final class FLDate extends UDF {
	
	private static SimpleDateFormat dayFormatStand = new SimpleDateFormat(
			"yyyy-MM-dd");
	private static SimpleDateFormat dayFormatStand2 = new SimpleDateFormat(
			"yyyyMMdd");

	public static SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
	public static Calendar calendar = Calendar.getInstance();

	public static void main(String[] args) {
		FLDate fld = new FLDate();
		Text t = new Text();
		t.set("20140812");
		System.out.println(fld.evaluate(t, new Text("month"), new Text("f")));
	}
	
	Text out = new Text();

	public Text evaluate(Text from ,Text period ,Text fol) {

		out.set("-");
		String fromStr = from.toString();
		String pStr = period.toString();
		String folStr = fol.toString();
		
		if("week".equals(pStr)) {
			if("f".equals(folStr)) {
				out.set(getWeekFirstDay(fromStr));
			} else if ("l".equals(folStr)) {
				out.set(getWeekLastDay(fromStr));
			}
		} else if ("month".equals(pStr)) {
			if("f".equals(folStr)) {
				out.set(getMonthFirstDay(fromStr));
			} else if ("l".equals(folStr)) {
				out.set(getMonthLastDay(fromStr));
			}
		}
		
		return out;
	}
	
	public static SimpleDateFormat getFormat(String from) {
		if(from.matches("^[0-9]+$") && from.length()==8) {
			return dayFormatStand2;
		}
		
		return dayFormatStand;
	}

	public static String getWeekFirstDay(String from) {
		String oldYearStr = "";
		try {
			// calendar.setTime(new Date(from.replace("-", "/")));
			calendar.setTime(getFormat(from).parse(from));
			oldYearStr = yearFormat.format(getFormat(from).parse(from));
		} catch (Exception e) {
			e.printStackTrace();
		}

		int mark = calendar.get(Calendar.DAY_OF_WEEK);

		if (mark == 1) {
			calendar.add(Calendar.WEEK_OF_MONTH,  - 1);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		} else {
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			calendar.add(Calendar.WEEK_OF_MONTH, 0);
		}
		String newYearStr = yearFormat.format(calendar.getTime());
		if (!newYearStr.equals(oldYearStr)) {
			return oldYearStr + "-01-01";
		} else {
			return getFormat(from).format(calendar.getTime());
		}

	}

	public static String getWeekLastDay(String from) {
		try {
			// calendar.setTime(new Date(from.replace("-", "/")));
			calendar.setTime(getFormat(from).parse(from));

		} catch (Exception e) {
			e.printStackTrace();
		}

		int mark = calendar.get(Calendar.DAY_OF_WEEK);

		if (mark == 1) {
			calendar.add(Calendar.WEEK_OF_MONTH, 0);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

		} else {
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			calendar.add(Calendar.WEEK_OF_MONTH,1);
		}

		return getFormat(from).format(calendar.getTime());

	}

	public static String getMonthFirstDay(String from) {

		try {
			calendar.setTime(getFormat(from).parse(from));
		} catch (Exception e) {
			e.printStackTrace();
		}
		calendar.set(Calendar.DAY_OF_MONTH, 1);

		return getFormat(from).format(calendar.getTime());

	}

	public static String getMonthLastDay(String from) {


		try {
			calendar.setTime(getFormat(from).parse(from));
		} catch (Exception e) {
			e.printStackTrace();
		}
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.add(Calendar.MONTH, 1);
		calendar.add(Calendar.DAY_OF_MONTH, -1);

		return getFormat(from).format(calendar.getTime());

	}


}
