package com.jnj.adf.xd.mutifiles.large.remoteservice;

import com.jnj.adf.grid.utils.JsonUtils;

public class SubmitResponseVo
{
  Integer code;
  String msg;
  String jobId;

  public SubmitResponseVo()
  {
    jobId = "";
    msg = "";
    code = 200;
  }

  public Integer getCode()
  {
    return code;
  }

  public void setCode(Integer code)
  {
    this.code = code;
  }

  public String getMsg()
  {
    return msg;
  }

  public void setMsg(String msg)
  {
    this.msg = msg;
  }

  public String getJobId()
  {
    return jobId;
  }

  public void setJobId(String jobId)
  {
    this.jobId = jobId;
  }

  public String toJson()
  {
    return JsonUtils.objectToJson(this);
  }

  @Override public String toString()
  {
    return "SubmitResponseVo{" + "code=" + code + ", msg='" + msg + '\'' + ", jobId='" + jobId + '\'' + '}';
  }
}
