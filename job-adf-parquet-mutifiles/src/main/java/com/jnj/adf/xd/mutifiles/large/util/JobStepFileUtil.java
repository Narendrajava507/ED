package com.jnj.adf.xd.mutifiles.large.util;

import java.io.Closeable;
import java.util.*;

public class JobStepFileUtil {
	
	public enum SystemType {
		HDFS,S3,FTP,LOCALS3;
		public static boolean contains(String type){
			for(SystemType st : SystemType.values()){
				if(st.toString().equalsIgnoreCase(type.toLowerCase())){
					return true;
				}
			}
			return false;
		}
		public boolean equalToMe(String type){
			return type!=null && this.name().equalsIgnoreCase(type.toLowerCase());
		}
	}
	
	public enum FileFormat {
		PARQUET("parquet"),CSV("csv"),AVRO("avsc"),JSON("json"), OPTYMYZE("optymyze");
		private String format = "";
		FileFormat(String text){
			this.format =text;
		}
		public static boolean contains(String type){
			for(FileFormat st : FileFormat.values()){
				if(st.toString().equalsIgnoreCase(type.toLowerCase())){
					return true;
				}
			}
			return false;
		}
		public boolean equalToMe(String type){
			return type!=null && this.name().equalsIgnoreCase(type.toLowerCase());
		}
		public static String getFileFormat(String type){
			if(type!=null){
				for(FileFormat st : FileFormat.values()){
					if(st.toString().equalsIgnoreCase(type.toLowerCase())){
						return st.getFormat();
					}
				}
			}
			return null;
		}
		public String getFormat(){
			return this.format;
		}
	}

	private EnumMap<SystemType, HashMap<String, List<String>>> fileList = new EnumMap<>(SystemType.class);
	
	private Map<String,String> parquetTrue = new HashMap<>();
	
	private Map<String,Closeable> parquetWriteList = new HashMap<>();
	
	private Map<String,Integer> serveSendCount = new HashMap<>();
	
	public static final JobStepFileUtil pfUtil = new JobStepFileUtil();
	
	private JobStepFileUtil(){
		for(SystemType st : SystemType.values()){
			fileList.put(st, new HashMap<String,List<String>>());
		}
	}
	
	public static JobStepFileUtil getInstance(){
		return pfUtil;
	}
	
	public synchronized void isParquetTrue(String key, String result){
		parquetTrue.put(key, result);
	}
	
	public synchronized String getParquetTrue(String key){
		return parquetTrue.remove(key);
	}
	
	public synchronized void putPath(SystemType systemType,String key,String filePath){
		Map<String,List<String>> list = fileList.get(systemType);
		if(!list.containsKey(key)){
			list.put(key, new ArrayList<String>());
		}
		list.get(key).add(filePath);
	}
	
	public List<String> getPathList(SystemType systemType,String key){
		Map<String,List<String>> list = fileList.get(systemType);
		return list.get(key);
	}
	
	public List<String> removePath(SystemType systemType,String key){
		Map<String,List<String>> list = fileList.get(systemType);
		return list.remove(key);
	}
	
	public void clearAllPath(SystemType systemType){
		Map<String,List<String>> list = fileList.get(systemType);
		list.clear();
	}
	
	public synchronized void setWriter(String key,Closeable write){
		parquetWriteList.put(key, write);
	}
	
	public Closeable getWriter(String key){
		return parquetWriteList.get(key);
	}
	
	public Closeable removeWriter(String key){
		return parquetWriteList.remove(key);
	}
	
	public boolean containWriter(String key){
		return parquetWriteList.containsKey(key);
	}
	
	public void putServerSendSize(String key,Integer count){
		serveSendCount.put(key, count);
	}
	
	public Integer getServerSendSize(String key){
		return serveSendCount.get(key);
	}

	public void removeStep(String key){
		parquetWriteList.remove(key);
		for(SystemType st : SystemType.values()){
			fileList.get(st).remove(key);
		}
	}
}
