package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function
 * <code>CONCAT_INT(sep, [int | array(int)]+)<code>.
 * This mimics the function from
 * MySQL http://dev.mysql.com/doc/refman/5.0/en/string-functions.html#
 * function_concat-ws
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "concat_int",
    value = "_FUNC_(separator, [int, array(int)]+) - "
    + "returns the concatenation of the strings separated by the separator.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('.', array(1, 2)) FROM src LIMIT 1;\n"
    + "  '1.2'")
public class ConcatInt extends GenericUDF {
    private ObjectInspector[] argumentOIs;
    public static final String STRING_TYPE_NAME = "string";
    public static final String INT_TYPE_NAME = "int";
    public static final String VOID_TYPE_NAME = "void";
    public static final String LIST_TYPE_NAME = "array";
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException(
                    "The function CONCAT_INT(separator,array(int)+) "
                    + "needs at least two arguments.");
        }

        if (!arguments[0].getTypeName().equals(STRING_TYPE_NAME)) {
            throw new UDFArgumentTypeException(0, "Argument 1 of CONCAT_INT (the separator) must be a string");
        }
        for (int i = 1; i < arguments.length; i++) {
            switch(arguments[i].getCategory()) {
                case LIST:
                    if (((ListObjectInspector)arguments[i]).getListElementObjectInspector()
                            .getTypeName().equals(INT_TYPE_NAME)
                            || ((ListObjectInspector)arguments[i]).getListElementObjectInspector()
                            .getTypeName().equals(VOID_TYPE_NAME))
                        break;
                case PRIMITIVE:
                    if (arguments[i].getTypeName().equals(INT_TYPE_NAME)
                            || arguments[i].getTypeName().equals(VOID_TYPE_NAME))
                        break;
                default:
                    throw new UDFArgumentTypeException(i, "Argument " + (i + 1)
                            + " of function CONCAT_INT must be \"" + INT_TYPE_NAME
                            + " or " + LIST_TYPE_NAME + "<" + INT_TYPE_NAME
                            + ">\", but \"" + arguments[i].getTypeName() + "\" was found.");
            }
        }

        argumentOIs = arguments;
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    private final Text resultText = new Text();

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }
        String separator = ((StringObjectInspector) argumentOIs[0])
            .getPrimitiveJavaObject(arguments[0].get());

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i = 1; i < arguments.length; i++) {
            if (arguments[i].get() != null) {
                if (first) {
                    first = false;
                } else {
                    sb.append(separator);
                }
                if (argumentOIs[i].getCategory().equals(Category.LIST)) {
                    Object strArray = arguments[i].get();
                    ListObjectInspector strArrayOI = (ListObjectInspector) argumentOIs[i];
                    boolean strArrayFirst = true;
                    for (int j = 0; j < strArrayOI.getListLength(strArray); j++) {
                        if (strArrayFirst) {
                            strArrayFirst = false;
                        } else {
                            sb.append(separator);
                        }
                        sb.append(strArrayOI.getListElement(strArray, j));
                    }
                } else {
                    sb.append(((StringObjectInspector) argumentOIs[i])
                            .getPrimitiveJavaObject(arguments[i].get()));
                }
            }
        }

        resultText.set(sb.toString());
        return resultText;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length >= 2);
        StringBuilder sb = new StringBuilder();
        sb.append("concat_ws(");
        for (int i = 0; i < children.length - 1; i++) {
            sb.append(children[i]).append(", ");
        }
        sb.append(children[children.length - 1]).append(")");
        return sb.toString();
    }
}