package cn.com.diditaxi.hive.cf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;

public class GetUserArticles extends UDF {

	private static final String USER_BLOG_URL = "http://internalapi.csdn.net/blog/article/get/articlelist?x-acl-token=A1rVKOFis7oaG2os3eeBNsdVum0K";

	public ArrayList<Text> evaluate(Text username) {
		return getUserBlogList(username.toString());
	}

	public static void main(String[] args) {
		GetUserArticles m = new GetUserArticles();
		Long a=System.currentTimeMillis();
		System.out.println("start");
		ArrayList<Text> res=m.evaluate(new Text("grhunter"));
		for(Text temp:res){
			System.out.println(temp);
		}
		Long b=System.currentTimeMillis();
		System.out.println("结果:"+res.size()+",耗时:"+(b-a));
		System.out.println("end");
	}

	public static ArrayList<Text> getUserBlogList(String userName) {
		ArrayList<Text> articles = new ArrayList<Text>();
		JSONObject obj=getUserBlogData(userName,"1","1");
		
		if(obj.isEmpty()){
			articles.add(new Text("44"));
			return articles;
		};
		String count = String.valueOf(obj.get("count"));

		if(Strings.isNullOrEmpty(count.trim()))  {
			articles.add(new Text("1 "+count));
			return articles;
		}
		
		if("null".equals(count.trim())) {
			articles.add(new Text("9 " +count));
			return articles;
		} else {
			
			articles.add(new Text("3 " +String.valueOf(obj.get("count"))));
			
		int total=Integer.parseInt(count);
		if(total > 23){	
			JSONArray  array =	obj.getJSONArray("list");
			JSONObject obj2=array.optJSONObject(0);
			String lastArticleTime=getStringNumber(String.valueOf(obj2.get("PostTime")));
			Long nowTime=(new Date()).getTime();
			Long tempTime=nowTime - Long.parseLong(lastArticleTime);
			long tt=tempTime.longValue() - 90*24*3600*1000l;
			if(tt <= 0){
				int page=1;
				while(total >= 100){
					JSONObject obj3=getUserBlogData(userName,page+"","100");
					JSONArray  array2 =	obj3.getJSONArray("list");
					for(int i=0;i<array2.size();i++){
						JSONObject obj4=array2.getJSONObject(i);
						articles.add(new Text("http://blog.csdn.net/"+userName+"/article/details/"+obj4.getString("ArticleId")));
					}
					total=total-100;
					page++;
				}
				if(total > 0 && total < 100){
					JSONObject obj3=getUserBlogData(userName,page+"","100");
					JSONArray  array2 =	obj3.getJSONArray("list");
					for(int i=0;i<array2.size();i++){
						JSONObject obj4=array2.getJSONObject(i);
						articles.add(new Text("http://blog.csdn.net/"+userName+"/article/details/"+obj4.getString("ArticleId")));
					}
				}
			}
		}
		}
		return articles;
		
	}
	
    public static String getStringNumber(String str) {
        return Pattern.compile("[^0-9]").matcher(str).replaceAll("");
    }
    
	private static JSONObject getUserBlogData(String userName,String page,String size){
		JSONObject result = new JSONObject();
		try {
			
		HttpClient httpClient = new HttpClient();
		httpClient.getHostConfiguration().setHost(getUrlHost(USER_BLOG_URL),
				80, "http");
		httpClient
				.getParams()
				.setParameter(
						"http.useragent",
						"Mozilla/5.0 (X11; U; Linux i686; zh-CN; rv:1.9.1.2) Gecko/20090803 Fedora/3.5.2-2.fc11 Firefox/3.5.2");

		HttpMethod method = null;
		
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("userName", userName);
			paramsMap.put("dataType", "json");
			paramsMap.put("size",size);
			paramsMap.put("page",page);
			method = getMethod(USER_BLOG_URL, paramsMap);
			httpClient.executeMethod(method);
			String response = method.getResponseBodyAsString();
			
			JSONObject arr = JSONObject.fromObject(response);
			String error=arr.getString("error");
			if(error.equals("")){
				result.put("count", "ss20");
				//result = arr.getJSONObject("data");
			}else{
				result.put("count", "ss21");
				//result=arr;
			}
				
			
		} catch (IOException e) {
			result.put("error", "IOE");
			result.put("count", "ss22");
			e.printStackTrace();
		}catch(Exception e){
			result.put("error", "E");
			result.put("count", "ss23");
			e.printStackTrace();
		}
		return result;
	}

	private static HttpMethod getMethod(String url, Map<String, String> params) {
		String paramStr = "";
		for (Map.Entry<String, String> entry : params.entrySet()) {
			paramStr += "&" + entry.getKey() + "=" + entry.getValue();
		}
		GetMethod get = new GetMethod(url + paramStr);
		get.releaseConnection();
		return get;
	}

	private static String getUrlHost(String url) {
		String host = "";
		if (url != null && !"".equals(url) && url.indexOf("http") != -1) {
			return url.split("/")[2];
		}
		return host;
	}

}
