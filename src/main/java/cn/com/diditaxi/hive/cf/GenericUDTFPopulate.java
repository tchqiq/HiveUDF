package cn.com.diditaxi.hive.cf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "populate",
    value = "_FUNC_(a) - populate struct input as single row")
public class GenericUDTFPopulate extends GenericUDTF {

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1 && args[0].getCategory() != ObjectInspector.Category.STRUCT) {
      throw new UDFArgumentException("populate() takes only one struct type argument");
    }
    return (StructObjectInspector) args[0];
  }

  @Override
  public void process(Object[] o) throws HiveException {
    forward(o[0]);
  }

  @Override
  public String toString() {
    return "explode";
  }
}
