package com.jnj.adf.xd.mutifiles.large.filewriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.jnj.adf.grid.common.ADFException;
import org.springframework.core.io.Resource;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.utils.LogUtil;

public class AdfOptymyzeWriter extends BasicWriter{

	private Resource resouce;
	private BufferedWriter writer;
	private String columnNames;
	
	public AdfOptymyzeWriter(Map<String,Object> params){
		resouce = (Resource)params.get("resouce");
		columnNames = (String)params.get("columnNames");
	}
	
	@Override
	public void close() throws IOException {
		if(writer!=null)
			writer.close();
	}

	@Override
	public void open()  {
		try {
			String path = resouce.getFile().getAbsolutePath();
			writer = new BufferedWriter(new FileWriter(path));

			LogUtil.getCoreLog().info("JSON file "+resouce.getURI().toString()+"is created!");
		} catch (IOException e) {
			throw new ADFException(e);
		}
	}

	@Override
	public void write(List<? extends JsonObject> items) {
		try {
			for(JsonObject item : items){
				StringBuilder sb = new StringBuilder();
				Map<String,Object> map = transformColumn(item).toMap();

				for(String col : columnNames.split(",")){
					sb.append(map.get(col)==null ? "":String.valueOf(map.get(col))).append("|");
				}
				if(sb.length()>0) {
					writer.write(sb.substring(0, sb.length()-1));
				}else{
					writer.write(sb.toString());
				}
				writer.newLine();
			}
		} catch (IOException e) {
			throw new ADFException(e);
		}
	}
}
