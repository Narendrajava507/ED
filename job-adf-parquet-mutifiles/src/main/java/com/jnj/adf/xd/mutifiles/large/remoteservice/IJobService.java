package com.jnj.adf.xd.mutifiles.large.remoteservice;

import com.jnj.adf.config.annotations.Path;
import com.jnj.adf.config.annotations.RemoteMethod;
import com.jnj.adf.config.annotations.RemoteServiceApi;

import java.util.List;

@RemoteServiceApi("com/jnj/adf/grid/recovery/JobService")
public interface IJobService {
    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> checkMetaRegions();

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public String acquireJobId(@Path String region, String folder, Long upt);

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> runJob(@Path String region, String jobId);

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> runJobForTest(@Path String region, String jobId);

    /**
     * get job status,return string ,one of waiting,running,failed,finished
     *
     * @param region
     * @param jobId
     * @return
     */
    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public String getJobStatus(@Path String region, String jobId);

    /**
     * remove job
     *
     * @param region
     * @return
     */
    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public String removeJob(@Path String region);

    /**
     * stop job
     *
     * @param region
     * @return
     */
    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> stopJob(@Path String region);

    /**
     * get running job
     *
     * @return
     */
    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public List<String> getRunningJob();

    /**
     * get pending job
     *
     * @return
     */
    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public List<String> getPendingJob();

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> stopAllTask();

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> stopTask(String taskId);

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVERS)
    public List<String> getAllRunningTaskId();

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public List<String> getJobTask(@Path String region, Long upt);

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public String getJobTaskById(String taskId);

    @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
    public List<String> getJobHistory(@Path String region);

}