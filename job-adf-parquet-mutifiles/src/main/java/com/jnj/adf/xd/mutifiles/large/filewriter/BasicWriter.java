package com.jnj.adf.xd.mutifiles.large.filewriter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.jnj.adf.client.api.JsonObject;

public abstract class BasicWriter implements AdfMutiFilesWriter{
	private Map<String,String> columnsMap = new HashMap<>();
	
	public void setColumnsMapping(String columnsMapping){
		columnsMap.clear();
		if(StringUtils.isNotBlank(columnsMapping)){
			columnsMapping = columnsMapping.trim();
			String[] mappings = columnsMapping.split(",",-1);
			for(String mapStr : mappings){
				if(StringUtils.isNotBlank(mapStr)){
					String[] maps = mapStr.split(":",-1);
					if(StringUtils.isNotBlank(maps[0]) && StringUtils.isNotBlank(maps[1])){
						columnsMap.put(maps[0].trim(), maps[1].trim());
					}
				}
			}
		}
	}
	
	public String getRealColumnName(String oldCol){
		if(columnsMap.containsKey(oldCol)){
			return columnsMap.get(oldCol);
		}
		return oldCol;
	}

	public String[] getRealColumnNames(String[] oldCols){
		if(columnsMap.size()>0 && oldCols.length>0){
			String[] newCols = new String[oldCols.length];
			for(int i=0;i< oldCols.length;i++){
				newCols[i]=getRealColumnName(oldCols[i]);
			}
			return newCols;
		}
		return oldCols;
	}
	
	public JsonObject transformColumn(JsonObject item){
		if(columnsMap.size()>0){
			JsonObject js = JsonObject.create();
			Map<String,Object> map = item.toMap();
			map.keySet().forEach(key -> {
				String col = getRealColumnName(key);
				js.append(col, map.get(key));
			});
			return js;
		}else{
			return item;
		}
	}

	public Map<String, String> getColumnsMap() {
		return columnsMap;
	}
}
