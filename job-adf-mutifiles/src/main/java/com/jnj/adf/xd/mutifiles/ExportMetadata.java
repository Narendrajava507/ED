package com.jnj.adf.xd.mutifiles;

import org.springframework.xd.module.options.spi.ModuleOption;

public class ExportMetadata {

	private int corePoolSize = 1;
	private int maxPoolSize = 1;
	private int commitInterval = 1000;
	private int batchSize = 32*1024;
	private String grid = "";
	private String namingServer = "";
	private String regionName = "";
	private int queueSize=10000;
	private Long maxReadWaitTime=5000L;
	private String queryString="*:*";
	private int sapCorePoolSize =3;
	private int sapMaxPoolSize =3;
	private String gridUser = "";
	private String gridPass ="";
	private String locators = "";
	private String resourcePath = "";
	private String target = "";
	private String format = "";
	private String hdfsLocation = "";
	private String kPrincipal = "";
	private String keyPath = "";
	private String hdfsFilePath = "";
	private String hdfsConfigPath = "";
	private String awsAccessKeyId = "";
	private String awsSecretAccessKey = "";
	private String s3Region = "";
	private String endPoint = "";
	private boolean deltaLoad = false;
	private String envName = "";
	private int maxLine = Integer.MAX_VALUE;
	private boolean skipGridColumns = true;
	private String skipColumns = "";
	private boolean useTimeSuffix = false;
	private String reportPath = "";
	private String columnsMapping = "";
	private String timestampCriterias = "";
	
	private String columns = "";
	
	private String ftpHost = "";
	private int ftpPort = 0;
	private String ftpUser = "";
	private String ftpPassword = "";
	private String ftpPath = "";
	
	private boolean statusFile = false;
	private boolean columnsValidation = false;
	
	private String fileNamePrefix = "";
	private String triggerFilePrefix = "";
	private String parquetVersion = "v2";
	private boolean UTF8Mode = false;
	private boolean dictionaryEncoding = true;


	public int getCorePoolSize() {
		return corePoolSize;
	}
	
	@ModuleOption("The Core Pool Size")
	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}
	
	public int getMaxPoolSize() {
		return maxPoolSize;
	}
	
	@ModuleOption("The Max Pool Size")
	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}
	
	public int getCommitInterval() {
		return commitInterval;
	}
	
	@ModuleOption("The Commit Interval")
	public void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	@ModuleOption("The Batch Size")
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public String getGrid() {
		return grid;
	}
	
	@ModuleOption("The Grid Name")
	public void setGrid(String grid) {
		this.grid = grid;
	}
	public String getNamingServer() {
		return namingServer;
	}
	
	@ModuleOption("The Naming Server")
	public void setNamingServer(String namingServer) {
		this.namingServer = namingServer;
	}
	
	public String getRegionName() {
		return regionName;
	}

	@ModuleOption("The Region Name")
	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	public int getQueueSize() {
		return queueSize;
	}

	@ModuleOption(value="read data queue size")
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public Long getMaxReadWaitTime() {
		return maxReadWaitTime;
	}

	@ModuleOption(value="THe max read wait time")
	public void setMaxReadWaitTime(Long maxReadWaitTime) {
		this.maxReadWaitTime = maxReadWaitTime;
	}

	public String getQueryString() {
		return queryString;
	}
	
	@ModuleOption(value="Query string")
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public int getSapCorePoolSize() {
		return sapCorePoolSize;
	}
	
	@ModuleOption(value="THe sap core pool size")
	public void setSapCorePoolSize(int sapCorePoolSize) {
		this.sapCorePoolSize = sapCorePoolSize;
	}

	public int getSapMaxPoolSize() {
		return sapMaxPoolSize;
	}
	
	@ModuleOption(value="THe sap max pool size")
	public void setSapMaxPoolSize(int sapMaxPoolSize) {
		this.sapMaxPoolSize = sapMaxPoolSize;
	}

	public String getGridUser() {
		return gridUser;
	}
	
	@ModuleOption(value="grid user")
	public void setGridUser(String gridUser) {
		this.gridUser = gridUser;
	}

	public String getGridPass() {
		return gridPass;
	}
	
	@ModuleOption(value="grid password")
	public void setGridPass(String gridPass) {
		this.gridPass = gridPass;
	}

	public String getLocators() {
		return locators;
	}
	
	@ModuleOption(value="grid locators")
	public void setLocators(String locators) {
		this.locators = locators;
	}

	public String getResourcePath() {
		return resourcePath;
	}
	
	@ModuleOption(value="parquet file path")
	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	public String getHdfsLocation() {
		return hdfsLocation;
	}

	@ModuleOption(value="hdfs location")
	public void setHdfsLocation(String hdfsLocation) {
		this.hdfsLocation = hdfsLocation;
	}

	public String getkPrincipal() {
		return kPrincipal;
	}

	@ModuleOption(value="hdfs kPrincipal ")
	public void setkPrincipal(String kPrincipal) {
		this.kPrincipal = kPrincipal;
	}

	public String getKeyPath() {
		return keyPath;
	}

	@ModuleOption(value="hdfs keyPath ")
	public void setKeyPath(String keyPath) {
		this.keyPath = keyPath;
	}

	public String getHdfsFilePath() {
		return hdfsFilePath;
	}

	@ModuleOption(value="the file path where the parquet files will be written to  ")
	public void setHdfsFilePath(String hdfsFilePath) {
		this.hdfsFilePath = hdfsFilePath;
	}

	public String getHdfsConfigPath() {
		return hdfsConfigPath;
	}

	@ModuleOption(value="the hdfs config path,find files that name end with .xml. Like core-site.xml")
	public void setHdfsConfigPath(String hdfsConfigPath) {
		this.hdfsConfigPath = hdfsConfigPath;
	}

	public String getAwsAccessKeyId() {
		return awsAccessKeyId;
	}

	@ModuleOption(value="s3 aws Access Key Id")
	public void setAwsAccessKeyId(String awsAccessKeyId) {
		this.awsAccessKeyId = awsAccessKeyId;
	}

	public String getAwsSecretAccessKey() {
		return awsSecretAccessKey;
	}

	@ModuleOption(value="s3 aws Secret Access Key")
	public void setAwsSecretAccessKey(String awsSecretAccessKey) {
		this.awsSecretAccessKey = awsSecretAccessKey;
	}

	public String getS3Region() {
		return s3Region;
	}

	@ModuleOption(value="s3 region")
	public void setS3Region(String s3Region) {
		this.s3Region = s3Region;
	}

	public String getEnvName() {
		return envName;
	}

	@ModuleOption(value="product : product , no product:nonproduct")
	public void setEnvName(String envName) {
		this.envName = envName;
	}

	public boolean isDeltaLoad() {
		return deltaLoad;
	}

	@ModuleOption(value="false : full-data , true:delta-data")
	public void setDeltaLoad(boolean deltaLoad) {
		this.deltaLoad = deltaLoad;
	}

	public String getTarget() {
		return target;
	}

	@ModuleOption(value="target system:S3 or HDFS")
	public void setTarget(String target) {
		this.target = target;
	}

	public String getFormat() {
		return format;
	}

	@ModuleOption(value="file format: CSV or PARQUET or AVRO or JSON")
	public void setFormat(String format) {
		this.format = format;
	}

	public int getMaxLine() {
		return maxLine;
	}

	@ModuleOption(value="single file max line,max value 2147483647")
	public void setMaxLine(int maxLine) {
		this.maxLine = maxLine;
	}
	
	public boolean isSkipGridColumns() {
		return skipGridColumns;
	}

	@ModuleOption(value="skip grid columns ? such as _DELETED_,_INST_,_INSUN_,_UPT_,_UPUN_")
	public void setSkipGridColumns(boolean skipGridColumns) {
		this.skipGridColumns = skipGridColumns;
	}

	public String getSkipColumns() {
		return skipColumns;
	}
	
	@ModuleOption(value="skip columns ?")
	public void setSkipColumns(String skipColumns) {
		this.skipColumns = skipColumns;
	}

	public boolean isUseTimeSuffix() {
		return useTimeSuffix;
	}

	@ModuleOption(value="user timestamp suffix in file name")
	public void setUseTimeSuffix(boolean useTimeSuffix) {
		this.useTimeSuffix = useTimeSuffix;
	}

	public String getReportPath() {
		return reportPath;
	}

	@ModuleOption(value="reportPath , not needed")
	public void setReportPath(String reportPath) {
		this.reportPath = reportPath;
	}

	public String getFtpHost() {
		return ftpHost;
	}

	@ModuleOption(value="ftp host")
	public void setFtpHost(String ftpHost) {
		this.ftpHost = ftpHost;
	}

	public int getFtpPort() {
		return ftpPort;
	}

	@ModuleOption(value="ftp port")
	public void setFtpPort(int ftpPort) {
		this.ftpPort = ftpPort;
	}

	public String getFtpUser() {
		return ftpUser;
	}

	@ModuleOption(value="ftp username")
	public void setFtpUser(String ftpUser) {
		this.ftpUser = ftpUser;
	}

	public String getFtpPassword() {
		return ftpPassword;
	}

	@ModuleOption(value="ftp password")
	public void setFtpPassword(String ftpPassword) {
		this.ftpPassword = ftpPassword;
	}

	public String getFtpPath() {
		return ftpPath;
	}

	@ModuleOption(value="ftp path")
	public void setFtpPath(String ftpPath) {
		this.ftpPath = ftpPath;
	}

	public boolean isStatusFile() {
		return statusFile;
	}

	@ModuleOption(value="need upload status file to edl ? ")
	public void setStatusFile(boolean statusFile) {
		this.statusFile = statusFile;
	}

	public boolean isColumnsValidation() {
		return columnsValidation;
	}

	@ModuleOption(value="whether need columns validation or not")
	public void setColumnsValidation(boolean columnsValidation) {
		this.columnsValidation = columnsValidation;
	}

	public String getColumnsMapping() {
		return columnsMapping;
	}

	@ModuleOption(value="columns mapping ,change column name in parquet. eg handoverDate:handover_date,xx:yy,aaa:bbb;a:c,b:e")
	public void setColumnsMapping(String columnsMapping) {
		this.columnsMapping = columnsMapping;
	}

	public String getTimestampCriterias() {
		return timestampCriterias;
	}

	@ModuleOption("TimestampCriterias,'aaa:1;bbb:3'")
	public void setTimestampCriterias(String timestampCriterias) {
		this.timestampCriterias = timestampCriterias;
	}

	public String getEndPoint() {
		return endPoint;
	}

	@ModuleOption("end point for locals3")
	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}

	public String getColumns() {
		return columns;
	}

	@ModuleOption("region columns")
	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getFileNamePrefix() {
		return fileNamePrefix;
	}

	@ModuleOption("filre prefix for optymyze file ")
	public void setFileNamePrefix(String fileNamePrefix) {
		this.fileNamePrefix = fileNamePrefix;
	}

	public String getTriggerFilePrefix() {
		return triggerFilePrefix;
	}

	@ModuleOption("first column for trigger file ")
	public void setTriggerFilePrefix(String triggerFilePrefix) {
		this.triggerFilePrefix = triggerFilePrefix;
	}

    public String getParquetVersion() {
        return parquetVersion;
    }

    @ModuleOption("parquet version , v1 or v2")
    public void setParquetVersion(String parquetVersion) {
        this.parquetVersion = parquetVersion;
    }

    public boolean isUTF8Mode() {
        return UTF8Mode;
    }

    @ModuleOption("parquet column is in UTF8 module ?")
    public void setUTF8Mode(boolean UTF8Mode) {
        this.UTF8Mode = UTF8Mode;
    }

	public boolean isDictionaryEncoding() {
		return dictionaryEncoding;
	}

	@ModuleOption("use dictionary encoding ?")
	public void setDictionaryEncoding(boolean dictionaryEncoding) {
		this.dictionaryEncoding = dictionaryEncoding;
	}
}