package com.jnj.adf.xd.mutifiles.filewriter;

import java.io.Closeable;
import java.util.List;


import com.jnj.adf.client.api.JsonObject;

public interface AdfMutiFilesWriter extends Closeable{
	
	void open();
	
	void write(List<? extends JsonObject> items);
}
