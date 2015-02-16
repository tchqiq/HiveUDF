package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * heqi 2014-06-19
 */
@Description(name = "row_num", 
			value = "_FUNC_(key) - 返回数据的行号",
			extended = "Example:\n"
		+ "  > SELECT row_num(key) FROM file_pv_track a;\n")

/**
 * 
 * The row_num function keeps track of last user key 
 * and simply increments the counter. 
 *
 */

@UDFType(deterministic = false)
public class RowNumber extends UDF 
{
	  private static int MAX_VALUE = 50;
	    private static String comparedColumn[] = new String[MAX_VALUE];
	    private static int rowNum = 1;
	   
	    public int evaluate(Object ...args) {
	        String columnValue[] = new String[args.length];
	        for (int i = 0; i < args.length; i++){
	            columnValue[i] = args[i].toString();
	        }
	        if (rowNum == 1) {
	            for (int i = 0; i < columnValue.length; i++)
	                comparedColumn[i] = columnValue[i];
	        }

	        for (int i = 0; i < columnValue.length; i++) {
	            if (!comparedColumn[i].equals(columnValue[i])) {
	                for (int j = 0; j < columnValue.length; j++) {
	                    comparedColumn[j] = columnValue[j];
	                }
	                rowNum = 1;
	                return rowNum++;
	            }
	        }
	        return rowNum++;
	    }
}
