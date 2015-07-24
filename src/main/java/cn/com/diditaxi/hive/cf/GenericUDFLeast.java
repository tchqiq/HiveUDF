package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

/**
 * GenericUDF Class for SQL construct "least(value1, value2, value3, ....)". 
 * Oracle's CREATEST returns the least of the list of one or more expressions.
 * 
 */
@Description(name = "least", value = "_FUNC_(value1, value2, value3, ....) " +
		"- Returns the least value in the list.", 
		extended = "Example:\n" + " > SELECT _FUNC_(2, 5, 12, 3) FROM src;\n 2")
@HivePdkUnitTests(setup = "create table dual_data (i int); "
		+ "insert overwrite table dual_data select 1 from dual limit 1;", 
		cleanup = "drop table if exists dual_data;", 
		cases = {
		@HivePdkUnitTest(query = "SELECT nexr_least(2, 5, 12, 3) " +
				"FROM dual_data;", result = "2"),
		@HivePdkUnitTest(query = "SELECT nexr_least('2', '5', '12', '3') " +
				"FROM dual_data;", result = "12"),
		@HivePdkUnitTest(query = "SELECT nexr_least('apples', 'oranges', 'bananas') " +
				"FROM dual_data;", result = "apples") })
public class GenericUDFLeast extends GenericUDF {

	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		argumentOIs = arguments;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		for (int i = 0; i < arguments.length; i++) {
			if (!returnOIResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i, "The value of return should have the same type: \""
						+ returnOIResolver.get().getTypeName() + 
						"\" is expected but \"" + arguments[i].getTypeName()
						+ "\" is found");
			}
		}

		return returnOIResolver.get();
	}

	@Override
	public Object evaluate(DeferredObject[] fields) throws HiveException {
		Object leastObject = null;
		ObjectInspector leastOI = null;

		for (int i = 0; i < fields.length; i++) {
			Object fieldObject = fields[i].get();
			if (leastObject == null) {
				leastObject = fieldObject;
				leastOI = argumentOIs[i];
				continue;
			}

			if (ObjectInspectorUtils.compare(leastObject, leastOI, fieldObject, argumentOIs[i]) >= 0) {
				leastObject = fieldObject;
				leastOI = argumentOIs[i];
			}
		}

		return returnOIResolver.convertIfNecessary(leastObject, leastOI);
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("least (");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}

}
