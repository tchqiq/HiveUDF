package cn.com.diditaxi.util;

import java.io.*;
import java.util.*;

public class ReadResourcesFile {

	/**
     * 读取资源文件
     * */
	public static List<String> readLines(String path){
		
    	List<String> lines = new ArrayList<String>();
        int i = 0;
        InputStream input = ReadResourcesFile.class.getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(input));
        String temp = null;
        try {
            while ((temp = br.readLine()) != null) {
                lines.add(temp);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    	return lines;
    }
}
