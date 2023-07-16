package com.jnj.adf.xd.mutifiles.large.remoteservice;

import com.jnj.adf.config.annotations.Path;
import com.jnj.adf.config.annotations.RemoteMethod;
import com.jnj.adf.config.annotations.RemoteServiceApi;

import java.util.List;

@RemoteServiceApi("RecoveryService")
public interface IMetaMappingService
{
  @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
  public List<String> getAllMapping();

  @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
  public String getMappingByRegion(@Path String fullPath);

  @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
  public String createAllMapping();

  @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
  public String createAllEmptyMappingByErp(String erpName);

  @RemoteMethod(type = RemoteMethod.InvokeTypes.ON_SERVER)
  public String updateMappingByRegion(@Path String fullPath, String json);
}
