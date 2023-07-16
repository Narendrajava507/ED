package com.jnj.adf.xd.mutifiles.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
//import com.gemstone.gemfire.cache.Region;
import org.apache.geode.cache.Region;
import com.jnj.adf.IndexerConstant;
import com.jnj.adf.client.api.ADFService;
import com.jnj.adf.grid.auth.AuthService;
import com.jnj.adf.grid.auth.session.AuthInternalSession;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.grid.manager.RegionHelper;
import com.jnj.adf.grid.support.system.ADFConfigHelper;
import com.jnj.adf.grid.support.system.ADFConfigHelper.ITEMS;
import com.jnj.adf.grid.utils.DateUtil;
import com.jnj.adf.xd.mutifiles.remoteservice.DataServiceApi;
import com.jnj.adf.xd.mutifiles.remoteservice.IBizExt;
import com.jnj.adf.xd.mutifiles.util.JobStepFileUtil;

public class RangePartitioner implements Partitioner {
	private static final Logger logger = LoggerFactory.getLogger(RangePartitioner.class);
	
	private String regionName;
	private String grid;
	private String namingServer;
	private String locators;
	private String username;
	private String password;
	@Autowired
	private AuthService authService;
	@Autowired
	ADFService adfService;
	
	private String connectGridInfo;
	private String queryString = "*:*";
	
	private boolean skipGridColumns = false;
	private String skipColumns;
	private String timestampCriterias;
	private String columnsMapping;
	
	private String columns;
	
	private DataServiceApi dataService;
	private List<String> skipSystemColumns = Arrays.asList("_DELETED_","_INST_","_INSUN_","_UPT_","_UPUN_");

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		logger.info("partition start");
		
		connectGrid();
		Map<String, ExecutionContext> result = new LinkedHashMap<>();
		try {
			logger.info("skipColumns:{}",skipColumns);
			
			dataService = IBizExt.getRemoteService(DataServiceApi.class);
			
			String[] regions = regionName.split(";");
			String[] querys = queryString.split(";");
			String[] skipColumnss = skipColumns.split(";", -1);
			String[] timestampCriteriass = timestampCriterias.split(";", -1);
			String[] columnsMappings = columnsMapping.split(";", -1);
			String[] columnss = columns.split(";", -1);

			//get columns from region
			for (int i=0;i<regions.length;i++){
				logger.info("Start query region：{}",regions[i]);
				
				Region<?, ?> region = RegionHelper.getRegion(regions[i]);
				if(region!=null){
					String query = getQueryString(querys,timestampCriteriass,i);
					long count = getRegionSize(regions[i],query);

					String skipColumnNames = skipColumnss.length>1 ? skipColumnss[i]:skipColumnss[0];
					String columnsMappingss = columnsMappings.length>1 ? columnsMappings[i]:columnsMappings[0];
					String columnsses = columnss.length>1 ? columnss[i]:columnss[0];
					
					String[] columnNamesAndKeys = getColumnByRegion(regions[i],skipColumnNames,columnsses);
					
					ExecutionContext value = new ExecutionContext();
					value.put("regionName", regions[i]);
					value.put("columnNames", columnNamesAndKeys[0]);
					value.put("keys", columnNamesAndKeys[1]);
					value.put(JobStepFileUtil.FILE_LIST_KEY, UUID.randomUUID().toString());
					value.put("queryString", query);
					value.put("count", String.valueOf(count));
					value.put("columnsMapping", columnsMappingss);
					result.put(regions[i], value);
				}else{
					throw new ADFException("Can not find region "+regions[i]);
				}
			}
		} catch (Exception e) {
			logger.error("partition error.",e);
			throw new ADFException(e);
		}
		return result;
	}

	private String getQueryString(String[] querys, String[] timestampCriteriass, int i) {
		String query = querys.length>1 ? querys[i]:querys[0];
		String timeQuery =  timestampCriteriass.length > 1 ? timestampCriteriass[i]:timestampCriteriass[0];
		
		if (!StringUtils.isBlank(timeQuery)){
			String[] timestampCriteria = timeQuery.split(":");
			long time = DateUtil.addDay(DateUtil.today(), -1 * Integer.valueOf(timestampCriteria[1]));
			String difDate = "'"+DateFormatUtils.format(time,"yyyyMMdd")+"'";
			
			String compare = " >= ";
			if("0".equals(timestampCriteria[1])) {
				compare = " == ";
			}
			if(timestampCriteria[0].equals("_UPT_")){
				compare = " >= ";
				difDate = String.valueOf(time);
			}
			
			String difDateOptions = new StringBuilder().append(timestampCriteria[0]).append(compare).append(difDate).toString();
			 
			if (!StringUtils.isBlank(query) && !"*:*".equals(query.trim())){
				query = "(" + query + ") && " + difDateOptions;
			} else {
				query = difDateOptions;
			}
		}
		return query;
	}

	private void connectGrid() {
		logger.info("Connect grid locators:{},namingServer:{}",locators,namingServer);
		ADFConfigHelper.setProperty(ITEMS.NAMING_SERVER, namingServer);

        AuthInternalSession tlSession = login();
		if (tlSession == null){
			connectGridInfo = "Login to grid failed, username is: " + username + ".";
		}
		
		logger.info("Connect grid success! {} ,namingServer:{}",locators,namingServer);
	}

	private AuthInternalSession login() {
		AuthInternalSession session = authService.login(username, password);
		connectGridInfo = "Connect to grid successful, username is:" + username + ".";
		logger.info(connectGridInfo);
		return session;
	}
	
	
	private long getRegionSize(String path, String query) {
		if(StringUtils.isBlank(query) || query.equals(IndexerConstant.Lucene.QUERY_ALL)){
            return dataService.getRegionSize(path);
		}else{
            return -1L;
		}
	}
	
	//get columns by region name 
	private String[] getColumnByRegion(String region,String skipColumns,String tcolumns) {
		logger.info("Query columns for {}",region);
		String[] keysAndCols = new String[2];
		StringBuilder sb = new StringBuilder();
		
		List<String> regionColumns = StringUtils.isNotBlank(tcolumns) ? new ArrayList<>(Arrays.asList(tcolumns.split(",")))
				: dataService.getRegionColumns(region);

		List<String> keys = dataService.getRegionKeys(region);
		
		
		for(String key : regionColumns){
			if(isSkipColumn(key,skipColumns)){
				continue;
			}
			sb.append(key).append(",");
		}
		
		logger.info("Get columns {}:{} ",region,sb);
		logger.info("Get keys {}:{} ",region,keys);
		
		keysAndCols[0] = sb.length()>0 ? sb.substring(0, sb.length()-1):"";
		keysAndCols[1] = StringUtils.join(keys.toArray(), ",");
		return keysAndCols;
	}

	//skip column ?
	private boolean isSkipColumn(String key,String skipColumn) {
		if(skipGridColumns && skipSystemColumns.contains(key)){
			return true;
		}
		if(StringUtils.isNotBlank(skipColumn)){
			String[] skips = skipColumn.split(",");
			for(String skip : skips){
				if(skip.trim().equals(key)){
					return true;
				}
			}
		}
		return false;
	}

	public String getRegionName() {
		return regionName;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	public String getGrid() {
		return grid;
	}

	public void setGrid(String grid) {
		this.grid = grid;
	}

	public String getNamingServer() {
		return namingServer;
	}

	public void setNamingServer(String namingServer) {
		this.namingServer = namingServer;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getLocators() {
		return locators;
	}

	public void setLocators(String locators) {
		this.locators = locators;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public boolean isSkipGridColumns() {
		return skipGridColumns;
	}

	public void setSkipGridColumns(boolean skipGridColumns) {
		this.skipGridColumns = skipGridColumns;
	}

	public String getSkipColumns() {
		return skipColumns;
	}

	public void setSkipColumns(String skipColumns) {
		this.skipColumns = skipColumns;
	}

	public String getTimestampCriterias() {
		return timestampCriterias;
	}

	public void setTimestampCriterias(String timestampCriterias) {
		this.timestampCriterias = timestampCriterias;
	}

	public String getColumnsMapping() {
		return columnsMapping;
	}

	public void setColumnsMapping(String columnsMapping) {
		this.columnsMapping = columnsMapping;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}
}
