package com.jnj.adf.springxd.grid.delete;


import java.util.List;

import com.jnj.adf.config.annotations.Path;
import com.jnj.adf.config.annotations.RemoteMethod;
import com.jnj.adf.config.annotations.RemoteMethod.InvokeTypes;
import com.jnj.adf.config.annotations.RemoteServiceApi;


@RemoteServiceApi(value="adf.data.region.dataDeleteService")
public interface IDataDeleteService
{
	@RemoteMethod(type = InvokeTypes.ON_SERVERS)
	public List<Integer> deleteDeltaData(@Path String regionName, String queryString, boolean isDelete, boolean isLuceneQuery, boolean bigTable, int batchSize, long timeout);

}
