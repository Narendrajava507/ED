package com.jnj.adf.springxd.grid.delete;


import com.jnj.adf.config.annotations.Path;
import com.jnj.adf.config.annotations.RemoteMethod;
import com.jnj.adf.config.annotations.RemoteMethod.InvokeTypes;
import com.jnj.adf.config.annotations.RemoteServiceApi;


@RemoteServiceApi(value="/adf/jnj/adf/support/QueueService")
public interface IQueueService
{
	@RemoteMethod(type = InvokeTypes.ON_SERVERS)
	public void disableQueue(@Path String path, boolean disable);

}
