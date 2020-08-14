package com.spd.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

public class HadoopFileUtil {


	public static HashMap<String,String> readTextFile(String txtFilePath, HashMap<String,String> map) {
		Configuration conf = new Configuration();
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(txtFilePath),conf);
			fsr = fs.open(new Path(txtFilePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null) {
				String[]arr = StringUtils.splitPreserveAllTokens(lineTxt, "\\|");
				map.put(arr[1],arr[0]);
			}
			System.out.println("MAP_SIZE|"+ map.size());
		}
		catch (Exception e) {
			System.out.println(e.toString());
		}
		finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return map;
	}

	//C0|m.95303.com
	public static HashMap<String,String> readTextString(String txtFilePath, HashMap<String,String> map) {
		String[] strings = txtFilePath.split(",");
		for (String lineTxt : strings){
			if (StringUtils.isNotBlank(lineTxt)){
				String[]arr = StringUtils.splitPreserveAllTokens(lineTxt, "\\|");
				map.put(arr[1],arr[0]);
			}
		}
         return map;
	}

	public static HashSet<String> readTextFile(String txtFilePath, HashSet<String> set) {
		Configuration conf = new Configuration();
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(txtFilePath),conf);
			fsr = fs.open(new Path(txtFilePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null)
				set.add(lineTxt.trim());
			System.out.println("SET_SIZE|"+ set.size());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					System.out.println(e.toString());
				}
			}
		}
		return set;
	}

	public static HashSet<String> readTextFileToUpperCase(String txtFilePath, HashSet<String> set) {
		Configuration conf = new Configuration();
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(txtFilePath),conf);
			fsr = fs.open(new Path(txtFilePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null)
				set.add(lineTxt.trim().toUpperCase());
			System.out.println("SET_SIZE|"+ set.size());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					System.out.println(e.toString());
				}
			}
		}
		return set;
	}

	public static boolean isFileExist (String path) {
		Configuration config = new Configuration();
		FileSystem fs;
		boolean fileExists = false;
		try {
			fs = FileSystem.get(config);
			fileExists = fs.exists(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(fileExists)
			return true;
		else
			return false;
	}

	public static boolean isFileExist(String src, Configuration conf) {
		if (conf == null) {
			return false;
		}
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			FileStatus[] fsta = fs.globStatus(new Path(src));
			if (fsta != null && fsta.length > 0)
				return fs.exists(fsta[0].getPath());
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void dus(String src, Configuration conf) {
		if (conf == null){
			System.out.println("Hadoop Conf Is Null !");
			return;
		}

		try{
			Path srcPath = new Path(src);
			FileSystem srcFs = srcPath.getFileSystem(conf);
			FileStatus status[] = srcFs.globStatus(new Path(src));
			if (status==null || status.length==0) {
				System.out.println(srcPath+"|No Such File Or Directory");
				return;
			}
			for(int i=0; i<status.length; i++) {
				long totalSize = srcFs.getContentSummary(status[i].getPath()).getLength();
				String pathStr = status[i].getPath().toString();
				System.out.println(("".equals(pathStr)?".":pathStr) + "\t" + totalSize/1024/1024/1024 +"GB");
			}
		}catch (Exception e){
			System.out.println(e.toString());
		}
	}

	public static boolean isDirectory (String path) {
		Configuration config = new Configuration();
		FileSystem fs;
		boolean fileExists = false;
		try {
			fs = FileSystem.get(config);
			fileExists = fs.isDirectory(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(fileExists)
			return true;
		else
			return false;

	}
}
