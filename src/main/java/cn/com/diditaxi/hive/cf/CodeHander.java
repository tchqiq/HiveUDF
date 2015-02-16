package cn.com.diditaxi.hive.cf;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.com.diditaxi.handler.Handler;

import com.google.common.collect.Maps;

public class CodeHander extends Handler {

	public final static String regexpStr = "^https://code\\.csdn\\.net/([a-zA-Z0-9_]{2,20})/(.*?$)";

	@Override
	public Map<String, String> HandleRequest(String request) {
		Map<String, String> output = Maps.newHashMap();

		Pattern p = Pattern.compile(regexpStr);
		Matcher m = p.matcher(request);

		if (m.find()) {

			output.put("article", m.group(2));
			output.put("type", "codeproject");
			output.put("author", m.group(1));

		} else if (successor != null) {

			output = successor.HandleRequest(request);

		}

		return output;
	}

}
