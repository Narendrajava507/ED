package com.jnj.adf.xd.mutifiles.large.filewriter;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.grid.utils.LogUtil;
import org.springframework.core.io.Resource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AdfJsonWriter extends BasicWriter {

    private Resource resouce;
    private BufferedWriter writer;

    AdfJsonWriter(Map<String, Object> params) {
        resouce = (Resource) params.get("resouce");
    }

    @Override
    public void close() throws IOException {
        if (writer != null)
            writer.close();
    }

    @Override
    public void open() {
        try {
            writer = new BufferedWriter(new FileWriter(resouce.getFile().getAbsolutePath()));
            LogUtil.getCoreLog().info("JSON file " + resouce.getURI().toString() + "is created!");
        } catch (IOException e) {
            throw new ADFException(e);
        }

    }

    @Override
    public void write(List<? extends JsonObject> items) {
        try {
            for (JsonObject item : items) {
                writer.write(transformColumn(item).toJson());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new ADFException(e);
        }
    }

}
