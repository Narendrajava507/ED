package com.jnj.adf.xd.mutifiles.large.listener;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.grid.utils.DateUtil;
import com.jnj.adf.springxd.job.util.HdfsUtils;
import com.jnj.adf.xd.mutifiles.large.util.CommonUtils;
import com.jnj.adf.xd.mutifiles.large.util.FtpUtil;
import com.jnj.adf.xd.mutifiles.large.util.JobStepFileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.ParquetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AdfParquetListener implements StepExecutionListener {
    private static final Logger logger = LoggerFactory.getLogger(AdfParquetListener.class);
    public static final String STEP_ID_FLAG = "step_id_flag";

    private String hdfsLocation = "";
    private String kPrincipal = "";
    private String keyPath = "";
    private String hdfsFilePath = "";
    private String hdfsConfigPath = "";

    private String awsAccessKeyId = "";
    private String awsSecretAccessKey = "";
    private String s3Region = "";
    private boolean deltaLoad = false;
    private String envName;

    private String ftpHost;
    private int ftpPort;
    private String ftpUser;
    private String ftpPassword;
    private String ftpPath;

    private String target;
    private String endPoint;

    private String reportPath;

    private boolean columnsValidation = true;

    private String format;

    private String fileNamePrefix = "";
    private String triggerFilePrefix = "";
    private String resourcePath;
    private boolean statusFile = false;
    boolean convertParquet = true;
    boolean dictionaryEncoding = true;

    @Autowired
    AdfJobListener jobListener;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        stepExecution.getExecutionContext().put(STEP_ID_FLAG, Long.toString(stepExecution.getId()));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        try {
            if (!StringUtils.equalsIgnoreCase(stepExecution.getStepName(), "adf_parquet_step")) {
                int stepSize = jobListener.getStepSize().decrementAndGet();
                Closeable remove = jobListener.getWriterMap().remove(Long.toString(stepExecution.getId()));
                if (remove != null) {
                    remove.close();
                }
                return writeTarget(stepExecution, stepSize);
            }
            return stepExecution.getExitStatus();
        } catch (Exception e) {
            logger.error("After step error ,", e);
            return ExitStatus.FAILED.addExitDescription(e);
        }
    }

    private ExitStatus writeTarget(StepExecution stepExecution, int stepSize) throws IOException {
        int readCount = stepExecution.getReadCount();
        int writeCount = stepExecution.getWriteCount();

        if (readCount != writeCount) {
            return ExitStatus.FAILED.addExitDescription("ReadCount not equals writeCount!");
        }

        if (!convertParquet) {
            String parquetPath = stepExecution.getExecutionContext().getString("parquetPath");
            int totallySize = (int) ParquetUtil.getParquetRowCount(parquetPath);
            stepExecution.setReadCount(totallySize);
            stepExecution.setWriteCount(totallySize);
            return stepExecution.getExitStatus();
        }

        return writeToTarget(stepExecution, stepSize);
    }

    private ExitStatus writeToTarget(StepExecution stepExecution, int stepSize) throws IOException {
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        String regionName = (String) executionContext.get("regionName");

        String filePath = jobListener.getFileMap().get(Long.toString(stepExecution.getId()));
        if (StringUtils.isNotBlank(filePath) && JobStepFileUtil.FileFormat.PARQUET.name().equalsIgnoreCase(format)) {
            long totallySize = ParquetUtil.getParquetRowCount(filePath);
            stepExecution.setReadCount((int) totallySize);
            stepExecution.setWriteCount((int) totallySize);

            if(!dictionaryEncoding){
                boolean b = com.jnj.adf.springxd.job.util.ParquetUtil.checkAndFixParquet(filePath);
                if (!b) {
                    return ExitStatus.FAILED.addExitDescription("Can not fix parquet " + filePath + "!");
                }
            }
        }

        if (target != null) {
            try {
                List<String> parquetFileList = new ArrayList<>();
                parquetFileList.add(filePath);
                if (stepExecution.getExitStatus().getExitCode().equals("COMPLETED") && stepExecution.getReadCount() > 0 && parquetFileList.size() > 0) {
                    return writeFileToTarget(regionName, parquetFileList, stepExecution, stepSize);
                }
            } catch (Exception e) {
                logger.error("Upload to target error:", e);
                return ExitStatus.FAILED.addExitDescription("File is done but can't upload to " + target.trim().toLowerCase() + "!").addExitDescription(e);
            }
        }
        return stepExecution.getExitStatus();
    }

    private ExitStatus writeFileToTarget(String regionName, List<String> parquetFileList, StepExecution stepExecution, int stepSize) {
        if (target.trim().equalsIgnoreCase("hdfs")) {
            return writeHdfs(regionName, parquetFileList, stepExecution, stepSize);
        }
        if (target.trim().equalsIgnoreCase("s3")) {
            return writeS3(regionName, parquetFileList, false, stepExecution);
        }
        if (target.trim().equalsIgnoreCase("locals3")) {
            return writeS3(regionName, parquetFileList, true, stepExecution);
        }
        if (target.trim().equalsIgnoreCase("ftp")) {
            return writeFtp(regionName, parquetFileList, stepExecution);
        }
        return stepExecution.getExitStatus();
    }

    private ExitStatus writeFtp(String regionName, List<String> parquetFileList, StepExecution stepExecution) {
        FtpUtil ftpUtil = null;
        try {
            logger.warn("Start uplaod {} file to {}", parquetFileList.size(), ftpHost);
            if (parquetFileList != null && !parquetFileList.isEmpty()) {
                ftpUtil = new FtpUtil(ftpHost, ftpPort, ftpUser, ftpPassword);
                if (!ftpUtil.isConnect())
                    throw new ADFException("connect error");

                String realPath = ftpPath.endsWith("/") ? ftpPath.substring(0, ftpPath.length() - 1) : ftpPath;

                for (String filePath : parquetFileList) {
                    File parquetFile = new File(filePath);
                    ftpUtil.uploadFile(parquetFile, realPath);
                }
            }
        } catch (Exception e) {
            logger.error("Upload to ftp error:", e);
            return ExitStatus.FAILED.addExitDescription("File is done but can't upload to ftp!").addExitDescription(e);
        } finally {
            if (ftpUtil != null)
                ftpUtil.close();
        }
        return stepExecution.getExitStatus();
    }

    public ExitStatus writeS3(String regionName, List<String> parquetFileList, boolean local, StepExecution stepExecution) {
        try {
            if (!parquetFileList.isEmpty()) {
                AmazonS3 s3 = null;
                if (local) {
                    AWSCredentials credentials1 = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
                    ClientConfiguration localClientConfig = new ClientConfiguration();
                    localClientConfig.setProtocol(Protocol.HTTP);
                    s3 = new AmazonS3Client(credentials1, localClientConfig);
                    s3.setEndpoint(endPoint);
                    S3ClientOptions s3opt = new S3ClientOptions();
                    s3opt.withPathStyleAccess(true);
                    s3.setS3ClientOptions(s3opt);
                } else {
                    AWSCredentials credentials = null;
                    credentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
                    ClientConfiguration clientConfig = new ClientConfiguration();
                    clientConfig.setProtocol(Protocol.HTTPS);
                    s3 = new AmazonS3Client(credentials, clientConfig);
                    Region r = Region.getRegion(Regions.fromName(s3Region));
                    s3.setRegion(r);
                }

                String bucketName = "eads-data-ingestion";
                String sePath = getS3PathForRegion(regionName);

                if (isDeltaLoad()) {
                    bucketName = new StringBuilder(bucketName).append("/unprocess/").append(envName).append("/delta-data/")
                            .append(DateUtil.format(new Date())).append("/").append(sePath).toString();
                } else {
                    bucketName = new StringBuilder(bucketName).append("/unprocess/").append(envName).append("/full-data/")
                            .append(sePath).toString();
                }

                for (String filePath : parquetFileList) {
                    File file = new File(filePath);
                    PutObjectRequest por = new PutObjectRequest(bucketName, file.getName(), file);
                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                    por.setMetadata(objectMetadata);
                    logger.info("Uploading {} to S3 from a file\n", filePath);
                    s3.putObject(por);
                }
            }
        } catch (Exception e) {
            logger.error("Upload to S3 error:", e);
            return ExitStatus.FAILED.addExitDescription("File is done but can't upload to S3!").addExitDescription(e);
        }
        return stepExecution.getExitStatus();
    }

    private String getS3PathForRegion(String regionName) {
        if (regionName.startsWith("/"))
            regionName = regionName.substring(1);
        if (regionName.endsWith("/"))
            regionName = regionName.substring(0, regionName.length() - 1);
        return regionName;
    }

    public ExitStatus writeHdfs(String regionName, List<String> parquetFileList, StepExecution stepExecution, int stepSize) {
        try {
            String realPath = CommonUtils.getPathWithRegionName(hdfsFilePath, regionName);
            int readCount = stepExecution.getReadCount();
            HdfsUtils hdfsUtils = new HdfsUtils();
            hdfsUtils.kerberosLogin(hdfsLocation, kPrincipal, keyPath, hdfsConfigPath);

            if (parquetFileList != null && !parquetFileList.isEmpty() && readCount > 0) {
                for (String filePath : parquetFileList) {
                    hdfsUtils.addFile(filePath, realPath);
                }
            }

            if (statusFile && stepSize <= 0) {
                pushStatusFile(makeStatusFile(regionName), regionName, hdfsUtils);
            }
        } catch (Exception e) {
            logger.error("Upload to HDFS error", e);
            return ExitStatus.FAILED.addExitDescription("File is done but can't upload to HDFS!").addExitDescription(e);
        }
        return stepExecution.getExitStatus();
    }


    private String makeStatusFile(String regionName) throws IOException {
        if (StringUtils.isNotBlank(resourcePath)) {
            String realPath = getEndFilePath(resourcePath, regionName);
            String endFilePath = new File(realPath, CommonUtils.getFileNameFromRegionName(regionName) + "_completed.parquet").getAbsolutePath();
            File endFile = new File(endFilePath);
            if (endFile.exists() && endFile.isFile()) {
                Files.deleteIfExists(endFile.toPath());
            }
            endFile.getParentFile().mkdirs();
            if (endFile.createNewFile()) {
                return endFile.getAbsolutePath();
            }
        }
        throw new ADFException("Can't create end file in " + resourcePath);
    }

    private String getEndFilePath(String resourcePath, String regionName) {
        String tregionName = CommonUtils.getRegionPath(regionName, false);

        String endPath = "";
        if (!resourcePath.endsWith("/")) {
            endPath = resourcePath + '/' + tregionName;
        } else {
            endPath = resourcePath + tregionName;
        }
        return endPath;
    }

    private void pushStatusFile(String endFilePath, String regionName, HdfsUtils hdfsUtils) throws IOException {
        if (StringUtils.isNotBlank(endFilePath)) {
            String realPath = CommonUtils.getPathWithRegionName(hdfsFilePath, regionName);
            hdfsUtils.addFile(endFilePath, realPath);
            if (StringUtils.isNotBlank(endFilePath)) {
                Files.delete(new File(endFilePath).toPath());
            }
        }
    }

    public String getHdfsLocation() {
        return hdfsLocation;
    }

    public void setHdfsLocation(String hdfsLocation) {
        this.hdfsLocation = hdfsLocation;
    }

    public String getkPrincipal() {
        return kPrincipal;
    }

    public void setkPrincipal(String kPrincipal) {
        this.kPrincipal = kPrincipal;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public void setKeyPath(String keyPath) {
        this.keyPath = keyPath;
    }

    public String getHdfsFilePath() {
        return hdfsFilePath;
    }

    public void setHdfsFilePath(String hdfsFilePath) {
        this.hdfsFilePath = hdfsFilePath;
    }

    public String getHdfsConfigPath() {
        return hdfsConfigPath;
    }

    public void setHdfsConfigPath(String hdfsConfigPath) {
        this.hdfsConfigPath = hdfsConfigPath;
    }

    public String getTarget() {
        return target;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public void setAwsAccessKeyId(String awsAccessKeyId) {
        this.awsAccessKeyId = awsAccessKeyId;
    }

    public String getAwsSecretAccessKey() {
        return awsSecretAccessKey;
    }

    public void setAwsSecretAccessKey(String awsSecretAccessKey) {
        this.awsSecretAccessKey = awsSecretAccessKey;
    }

    public String getS3Region() {
        return s3Region;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public String getEnvName() {
        return envName;
    }

    public void setEnvName(String envName) {
        this.envName = envName;
    }

    public boolean isDeltaLoad() {
        return deltaLoad;
    }

    public void setDeltaLoad(boolean deltaLoad) {
        this.deltaLoad = deltaLoad;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getReportPath() {
        return reportPath;
    }

    public void setReportPath(String reportPath) {
        this.reportPath = reportPath;
    }

    public String getFtpHost() {
        return ftpHost;
    }

    public void setFtpHost(String ftpHost) {
        this.ftpHost = ftpHost;
    }

    public int getFtpPort() {
        return ftpPort;
    }

    public void setFtpPort(int ftpPort) {
        this.ftpPort = ftpPort;
    }

    public String getFtpUser() {
        return ftpUser;
    }

    public void setFtpUser(String ftpUser) {
        this.ftpUser = ftpUser;
    }

    public String getFtpPassword() {
        return ftpPassword;
    }

    public void setFtpPassword(String ftpPassword) {
        this.ftpPassword = ftpPassword;
    }

    public String getFtpPath() {
        return ftpPath;
    }

    public void setFtpPath(String ftpPath) {
        this.ftpPath = ftpPath;
    }

    public boolean isColumnsValidation() {
        return columnsValidation;
    }

    public void setColumnsValidation(boolean columnsValidation) {
        this.columnsValidation = columnsValidation;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    public String getTriggerFilePrefix() {
        return triggerFilePrefix;
    }

    public void setTriggerFilePrefix(String triggerFilePrefix) {
        this.triggerFilePrefix = triggerFilePrefix;
    }

    public boolean isStatusFile() {
        return statusFile;
    }

    public void setStatusFile(boolean statusFile) {
        this.statusFile = statusFile;
    }

    public boolean isConvertParquet() {
        return convertParquet;
    }

    public void setConvertParquet(boolean convertParquet) {
        this.convertParquet = convertParquet;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public boolean isDictionaryEncoding() {
        return dictionaryEncoding;
    }

    public void setDictionaryEncoding(boolean dictionaryEncoding) {
        this.dictionaryEncoding = dictionaryEncoding;
    }
}
