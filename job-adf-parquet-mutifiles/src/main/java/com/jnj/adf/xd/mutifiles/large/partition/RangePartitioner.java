package com.jnj.adf.xd.mutifiles.large.partition;

//import com.jnj.adf.client.api.ADFService;
import com.jnj.adf.client.api.IRemoteService;
import com.jnj.adf.config.annotations.EnableADF;

import com.jnj.adf.grid.auth.AuthService;
import com.jnj.adf.grid.auth.session.AuthInternalSession;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.grid.security.ISecurityConfigService;

import com.jnj.adf.grid.support.system.ADFConfigHelper;
import com.jnj.adf.grid.support.system.ADFConfigHelper.ITEMS;
import com.jnj.adf.xd.mutifiles.large.listener.AdfJobListener;
import com.jnj.adf.xd.mutifiles.large.remoteservice.DataServiceApi;
import com.jnj.adf.xd.mutifiles.large.remoteservice.IBizExt;
import com.jnj.adf.xd.mutifiles.large.util.ParquetServer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.gemfire.remoteService.EnableRemoteServiceExecutions;

import java.io.File;
import java.util.*;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

@EnableADF
@EnableRemoteServiceExecutions(basePackages = "com.jnj.adf.xd.mutifiles.remoteService")
public class RangePartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(RangePartitioner.class);

    private String regionName;
    private String namingServer;
    private String locators;
    private String username;
    private String password;
    private String parquetStorePath;
    private String queryString = "*:*";
    private boolean skipGridColumns = false;
    private String skipColumns;
    private String columnsMapping;
    private String fileNameSuffix = ".parquet";
    private String columns;
    private boolean skipCreateParquet = false;

    @Autowired
    private AdfJobListener jobListener;
    @Autowired
    private AuthService authService;
	/*
	 * @Autowired ADFService adfService;
	 */
    private DataServiceApi dataService;
    private String connectGridInfo;

    private boolean columnsValidation = false;

    private List<String> skipSystemColumns = Arrays
            .asList("_DELETED_", "_INST_", "_INSUN_", "_UPT_", "_UPUN_", "_PK_");

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        logger.info("partition start");
        Map<String, ExecutionContext> result = new HashMap<>();

        try {
            connectGrid();
            dataService = IBizExt.getRemoteService(DataServiceApi.class);

            Long regionSize = dataService.getRegionSize(regionName);
            if (regionSize == null || regionSize <= 0) {
                return result;
            }

            String[] columnNamesAndKeys = getColumnByRegion(regionName, skipColumns, columns);

            String parquetPaths = new ParquetServer(jobListener).createParquetOnServer(parquetStorePath, regionName, skipCreateParquet);

            if (StringUtils.isBlank(parquetPaths)) {
                throw new ADFException(
                        "Can not create parquet files for " + regionName + " in " + parquetStorePath);
            }

            File[] files = getParquetFiles(parquetPaths);

            int index = 0;
            for (File pfile : files) {
                index++;
                ExecutionContext value = new ExecutionContext();
                value.put("regionName", regionName);
                value.put("columnNames", columnNamesAndKeys[0]);
                value.put("keys", columnNamesAndKeys[1]);
                value.put("queryString", queryString);
                value.put("columnsMapping", columnsMapping);
                value.put("parquetPath", pfile.getAbsolutePath());
                value.put("parquetStorePath", parquetStorePath);
                value.put("MultiParquetItemWriter.resource.index", index);
                result.put(pfile.getName(), value);
            }
            jobListener.getStepSize().set(result.size());
        } catch (Exception e) {
            logger.error("partition end by exception");
            throw new ADFException(e);
        }
        return result;
    }

    private File[] getParquetFiles(String parquetPaths) {
        File[] files = new File(parquetPaths).listFiles(file -> file.isFile() && file.getAbsolutePath().toLowerCase().endsWith(fileNameSuffix.toLowerCase()));
        if (files != null && files.length > 0) {
            return files;
        } else {
            throw new ADFException("No parquet files in " + parquetPaths);
        }
    }

    // get columns by region name
    private String[] getColumnByRegion(String region, String skipColumns, String tcolumns) {
        logger.info("Query columns for {}", region);
        String[] keysAndCols = new String[2];
        StringBuilder sb = new StringBuilder();

        List<String> serviceColumns = dataService.getRegionColumns(region);
        if (columnsValidation) {
            checkVersionColumns(serviceColumns);
        }
        List<String> regionColumns = StringUtils.isNotBlank(tcolumns) ? new ArrayList<>(Arrays.asList(tcolumns.split(","))) : serviceColumns;
        List<String> keys = dataService.getRegionKeys(region);
        for (String key : regionColumns) {
            if (isSkipColumn(key, skipColumns)) {
                continue;
            }
            sb.append(key).append(",");
        }

        logger.info("Get columns {}:{} ", region, sb);
        logger.info("Get keys {}:{} ", region, keys);

        keysAndCols[0] = sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
        keysAndCols[1] = StringUtils.join(keys.toArray(), ",");
        return keysAndCols;
    }

    private void checkVersionColumns(List<String> regionColumns) {
        IRemoteService<ISecurityConfigService> irs = IBizExt.getIRemoteService(ISecurityConfigService.class);
        Set<String> versionColumns = irs.onServer(regionName).getVersionColumns(regionName);
        if (versionColumns.size() > 0) {
            if (versionColumns.size() != regionColumns.size()) {
                throw new ADFException("Version columns size and region columns size are not same.");
            }
            for (String regionColumn : regionColumns) {
                if (!versionColumns.contains(regionColumn)) {
                    throw new ADFException("Column " + regionColumn + " is not in version columns.");
                }
                versionColumns.remove(regionColumn);
            }
            if (versionColumns.size() > 0) {
                throw new ADFException("Version columns [" + StringUtils.join(versionColumns, ",") + "] is not in region columns.");
            }
        }
    }

    // skip column ?
    private boolean isSkipColumn(String key, String skipColumn) {
        if (skipGridColumns && skipSystemColumns.contains(key)) {
            return true;
        }
        if (StringUtils.isNotBlank(skipColumn)) {
            String[] skips = skipColumn.split(",");
            for (String skip : skips) {
                if (skip.trim().equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void connectGrid() {
        logger.info("Connect grid locators:{} ,namingServer:{}", locators, namingServer);
        ADFConfigHelper.setProperty(ITEMS.NAMING_SERVER, namingServer);

        AuthInternalSession tlSession = login();
        if (tlSession == null) {
            connectGridInfo = "Login to grid failed, username is: " + username + ".";
        }

        logger.info("Connect grid success! {} ,namingServer:{}", locators, namingServer);
    }

    private AuthInternalSession login() {
        AuthInternalSession session = authService.login(username, password);
        connectGridInfo = "Connect to grid successful, username is:" + username + ".";
        logger.info(connectGridInfo);
        return session;
    }

    private static List<String> getParquetFields(String parquetfieDir) {
        try {
            File dir = new File(parquetfieDir);
            if (dir.exists()) {
                File[] files = dir.listFiles();
                if (files.length > 0) {
                    for (File pFile : files) {
                        if (pFile.isFile() && pFile.getAbsolutePath().endsWith(".parquet")) {
                            Configuration configuration = new Configuration();
                            ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, new Path(pFile.getAbsolutePath()), NO_FILTER);
                            List<Type> fields = readFooter.getFileMetaData().getSchema().getFields();

                            List<String> fieldNames = new ArrayList<>();
                            fields.stream().forEach(type -> fieldNames.add(type.getName()));

                            return fieldNames;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("get parquet fields error.", e);
        }
        return new ArrayList<>();
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
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

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
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

    public List<String> getSkipSystemColumns() {
        return skipSystemColumns;
    }

    public void setSkipSystemColumns(List<String> skipSystemColumns) {
        this.skipSystemColumns = skipSystemColumns;
    }

    public String getParquetStorePath() {
        return parquetStorePath;
    }

    public void setParquetStorePath(String parquetStorePath) {
        this.parquetStorePath = parquetStorePath;
    }

    public String getFileNameSuffix() {
        return fileNameSuffix;
    }

    public void setFileNameSuffix(String fileNameSuffix) {
        this.fileNameSuffix = fileNameSuffix;
    }

    public boolean isSkipCreateParquet() {
        return skipCreateParquet;
    }

    public void setSkipCreateParquet(boolean skipCreateParquet) {
        this.skipCreateParquet = skipCreateParquet;
    }

    public boolean isColumnsValidation() {
        return columnsValidation;
    }

    public void setColumnsValidation(boolean columnsValidation) {
        this.columnsValidation = columnsValidation;
    }
}
