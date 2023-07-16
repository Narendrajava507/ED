package com.jnj.adf.xd.mutifiles.large.util;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.grid.utils.JsonUtils;
import com.jnj.adf.xd.mutifiles.large.listener.AdfJobListener;
import com.jnj.adf.xd.mutifiles.large.remoteservice.IBizExt;
import com.jnj.adf.xd.mutifiles.large.remoteservice.IJobService;
import com.jnj.adf.xd.mutifiles.large.remoteservice.IMetaMappingService;
import com.jnj.adf.xd.mutifiles.large.remoteservice.SubmitResponseVo;

public class ParquetServer {
    private static final Logger logger = LoggerFactory.getLogger(ParquetServer.class);

    private AdfJobListener jobListener;

    private IJobService jobService;
    private IMetaMappingService mappingService;

    public ParquetServer(AdfJobListener jobListener) {
        jobService = IBizExt.getRemoteService(IJobService.class);
        mappingService = IBizExt.getRemoteService(IMetaMappingService.class);
        this.jobListener = jobListener;
    }

    public String createParquetOnServer(String filePath, String regionName, boolean skipCreateParquet)
            throws Exception {
        logger.warn("Starting to crate parquets for {} in {} ,", regionName, filePath);
        if (!skipCreateParquet) {
            String jobId = acquireJobId(filePath, regionName);
            runJob(regionName, jobId);
            return waitingJob(regionName, jobId);
        } else {
            return getFolderFromRegion(regionName);
        }
    }

    public String acquireJobId(String filePath, String regionName) {
        logger.warn("Starting to checkMetaRegions for {}", regionName);
        checkRegionMeta(regionName);
        // delete not finished if exists
        logger.warn("Starting to removeJob for {}", regionName);
        jobService.removeJob(regionName);
        logger.warn("Starting to acquireJobId for {}", regionName);
        String json = jobService.acquireJobId(regionName, filePath, System.currentTimeMillis());
        SubmitResponseVo vo = JsonUtils.jsonToObject(json, SubmitResponseVo.class);
        if (vo.getCode().intValue() != 200) {
            throw new ADFException("Can not acquire job id ,code " + vo.getCode().intValue() + ",message:" + vo.getMsg());
        }
        logger.warn("Success to acquireJobId for {},jobId {}", regionName, vo.getJobId());
        return vo.getJobId();
    }

    private void checkRegionMeta(String regionName) {
        IBizExt.getIRemoteService(IJobService.class).onServers(regionName).checkMetaRegions();
    }

    public void runJob(String regionName, String jobId) {
        List<String> list = jobService.runJob(regionName, jobId);
        if (list != null) {
            for (String rs : list) {
                String[] ss = rs.split("@@");
                if (!StringUtils.equals("true", ss[1])) {
                    throw new ADFException("Not all server run job successfully , " + rs);
                }
            }
        } else {
            throw new ADFException("Can not get whether job is runing ");
        }
        logger.warn("Success to runJob for {},jobId {}", regionName, jobId);
    }

    public String waitingJob(String regionName, String jobId) throws Exception {
        String status = jobService.getJobStatus(regionName, jobId);
        // if failed
        while (!StringUtils.equals(status, "finished") && !StringUtils.equals(status, "failed")) {
            Thread.sleep(5000);
            if (jobIsRunning()) {
                status = jobService.getJobStatus(regionName, jobId);
                logger.info("export {} ,jobId:{} status:{}", regionName, jobId, status);
            } else {
                jobService.stopJob(regionName);
                logger.error("Export job stopped since XD job was stopped {}.", jobIsRunning());
                throw new ADFException("Export job stopped since XD job was stopped.");
            }
        }
        if (!StringUtils.equals(status, "finished")) {
            throw new ADFException("Can not generated parquet foler status {" + status + "}");
        }
        // check result
        return getFolderFromRegion(regionName);
    }

    private boolean jobIsRunning() throws Exception {
        if (jobListener == null) {
            return false;
        }
        String exitStatus = jobListener.jobService().getJobExecution(jobListener.getJobExecution().getId()).getExitStatus().getExitCode();
        if (!(exitStatus.equals(ExitStatus.EXECUTING.getExitCode())
                || exitStatus.equals(ExitStatus.UNKNOWN.getExitCode()))) {
            logger.error("Export job will stop since job exitstatus {} is not EXECUTING", exitStatus);
            return false;
        }
        String batchStatus = jobListener.jobService().getJobExecution(jobListener.getJobExecution().getId()).getStatus().name();
        if (!batchStatus.equals(BatchStatus.STARTED.name())
                && !batchStatus.equals(BatchStatus.STARTING.name())
                && !batchStatus.equals(BatchStatus.UNKNOWN.name())) {
            logger.error("Export job will stop since job batchstatus {} is not STARTED", batchStatus);
            return false;
        }
        logger.info("Export job status {} {},", exitStatus, batchStatus);
        return true;
    }

    public String getFolderFromRegion(String regionName) {
        String json = mappingService.getMappingByRegion(regionName);
        String folder = JsonObject.append(json).getValue("absolutePath");
        if (StringUtils.isBlank(folder)) {
            throw new ADFException("Can not generated parquet foler.JsonString:" + json);
        }
        logger.warn("Finished to crate parquets for {} in {} ,", regionName, folder);
        return folder;
    }
}
