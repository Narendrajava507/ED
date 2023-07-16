/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jnj.adf.xd.mutifiles.writer;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.file.ResourceSuffixCreator;

import com.jnj.adf.xd.mutifiles.util.JobStepFileUtil;


/**
 * Trivial implementation of {@link ResourceSuffixCreator} that uses the index
 * itself as suffix, separated by dot.
 * 
 * @author sli172
 */
public class ParquetSuffixCreator implements ResourceSuffixCreator {

	private String regionName;
	private String suffix = "parquet";
	private boolean useTimeSuffix = true; 
	
	private String fileNamePrefix = "";
	
    public String getRegionName() {
		return regionName;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}
	
	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}
	
	public boolean isUseTimeSuffix() {
		return useTimeSuffix;
	}

	public void setUseTimeSuffix(boolean useTimeSuffix) {
		this.useTimeSuffix = useTimeSuffix;
	}

	public String getFileNamePrefix() {
		return fileNamePrefix;
	}

	public void setFileNamePrefix(String fileNamePrefix) {
		this.fileNamePrefix = fileNamePrefix;
	}

	@Override
	public String getSuffix(int index) {
		if(JobStepFileUtil.FileFormat.OPTYMYZE.equalToMe(suffix)){
			return getOptymyzeType();
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		String timestamp = sdf.format(new Date());
		String name = regionName.replace("/", "_");
		if ( !name.startsWith("_") && !name.startsWith(".")){
			if(useTimeSuffix){
				return File.separator + name + "_" + index + "_" + timestamp + "." + getRealSuffix().toLowerCase();
			}else{
				return File.separator + name + "_" + index +"." + getRealSuffix().toLowerCase();
			}
		}
		if(useTimeSuffix){
			return File.separator + name.substring(1) + "_" + index + "_" + timestamp + "." + getRealSuffix().toLowerCase();
		}else{
			return File.separator + name.substring(1) + "_" + index + "." + getRealSuffix().toLowerCase();
		}
	}
	
	public String getOptymyzeType(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String timestamp = sdf.format(new Date());
		String name = StringUtils.isNotBlank(fileNamePrefix) ? fileNamePrefix:regionName.replace("/", "_");
		
		if ( !name.startsWith("_") && !name.startsWith(".")){
			return File.separator + name.toUpperCase() + "_DETL_"  + timestamp + ".TXT";
		}else{
			return File.separator + name.substring(1).toUpperCase() + "_DETL_"  + timestamp + ".TXT";
		}
	}
	
	public String getRealSuffix(){
		return JobStepFileUtil.FileFormat.getFileFormat(suffix);
	}
}
