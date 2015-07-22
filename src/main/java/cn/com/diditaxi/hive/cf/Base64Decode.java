package cn.com.diditaxi.hive.cf;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Base64Decode extends UDF {

    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        byte[] base64hash = Base64.decodeBase64(s.getBytes());
        return new Text(new String(base64hash));
    }
}