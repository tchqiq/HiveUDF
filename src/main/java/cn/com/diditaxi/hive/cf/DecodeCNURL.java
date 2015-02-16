package cn.com.diditaxi.hive.cf;

import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.common.base.Strings;

/**
 * heqi 2014-06-17
 */
@Description(name = "decode_url", 
			value = "_FUNC_(q) - 解析中文url",
			extended = "Example:\n"
		+ "  > SELECT decode_url(q) FROM file_pv_track a;\n")

/**
 * 
 * The decode function simply decode the ZN url 
 *
 */
public final class DecodeCNURL extends UDF {
	public static final String NULL_STRING = "";
	private static final String SEARCH_GBK_ENGINE_REGEX = ".*(3721|iask|sogou|163|baidu|soso|zhongsou).*";
	private static final String SEARCH_TWO_LANGUAGE_ENGINE_REGEX = ".*(yahoo|google).*";
	private static final String SEARCH_TWO_LANGUAGE_ENGINE_REGEX_FLAG = "ie=utf";
	private static String whiteList[] = new String[] { "：" };
	private static final String UTF = "utf-8";
	private static final String GBK = "gbk";

	public static void main(String[] args) {
		String s = new DecodeCNURL().evaluate("%E6%98%BE%E7%A4%BA%E6%9B%B4%E5%A4%9A%2Cjavascript%3Avoid(0)%2C-");
		System.out.println(s);
	}
	
	public String evaluate(final String query) {
		if (Strings.isNullOrEmpty(query)) {
			return null;
		}
		return decodeAndCheck(query, "");
	}
	
	
	public static String decodeAndCheck(String keySrc, String type) {

		if (keySrc != null) {

			String key = null;

			keySrc = keySrc.replace("%25", "%");
			String encode = UTF;
			type = type.toLowerCase();

			if (type.indexOf(SEARCH_TWO_LANGUAGE_ENGINE_REGEX_FLAG) > 0) {

				encode = UTF;

			} else if ((type.matches(SEARCH_GBK_ENGINE_REGEX))) {

				encode = GBK;
			}
			try {
				key = URLDecoder.decode(keySrc, encode);

			} catch (Exception e) {
				key = NULL_STRING;
			}

			return key;

		} else {
			return keySrc;
		}

	}

}
