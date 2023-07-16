package com.jnj.adf.xd.mutifiles.large.writer;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.xd.mutifiles.large.filewriter.AdfMutiFilesWriterFactory;
import com.jnj.adf.xd.mutifiles.large.filewriter.BasicWriter;
import com.jnj.adf.xd.mutifiles.large.listener.AdfJobListener;
import com.jnj.adf.xd.mutifiles.large.listener.AdfParquetListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdfMutiFilesJobWriter extends AbstractItemStreamItemWriter<JsonObject>
        implements ResourceAwareItemWriterItemStream<JsonObject> {
    private static final Logger logger = LoggerFactory.getLogger(ResourceAwareItemWriterItemStream.class);

    private String columnNames;
    private String regionName;
    private String target;
    private String format;
    private String columnsMapping;
    private String keys;
    private boolean UTF8Mode;
    private String stepId = null;
    private Resource resouce;
    private BasicWriter writer;
    private boolean convertParquet;

    @Autowired
    AdfJobListener jobListener;

    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);
        Map<String, Object> params = new HashMap<>();
        params.put("columnNames", columnNames);
        params.put("regionName", regionName);
        params.put("resouce", resouce);
        params.put("keys", keys);
        params.put("UTF8Mode", UTF8Mode);
        params.put("convertParquet", convertParquet);

        try {
            stepId = executionContext.getString(AdfParquetListener.STEP_ID_FLAG);
            writer = AdfMutiFilesWriterFactory.getWriter(format, executionContext, params);
            writer.setColumnsMapping(columnsMapping);
            writer.open();
            jobListener.getFileMap().put(stepId, resouce.getFile().getAbsolutePath());
            jobListener.getWriterMap().put(stepId, writer);
        } catch (Exception e) {
            logger.error("get writer error ", e);
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void write(List<? extends JsonObject> items) throws Exception {
        writer.write(items);
    }

    @Override
    public void close() {
        try {
            Closeable writer = jobListener.getWriterMap().remove(stepId);
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            logger.error("Close writer error.", e);
            throw new ADFException(e);
        }
    }

    @Override
    public void setResource(Resource resource) {
        this.resouce = resource;
    }

    public String getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String columnNames) {
        this.columnNames = columnNames;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Resource getResouce() {
        return resouce;
    }

    public void setResouce(Resource resouce) {
        this.resouce = resouce;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getColumnsMapping() {
        return columnsMapping;
    }

    public void setColumnsMapping(String columnsMapping) {
        this.columnsMapping = columnsMapping;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public boolean isUTF8Mode() {
        return UTF8Mode;
    }

    public void setUTF8Mode(boolean UTF8Mode) {
        this.UTF8Mode = UTF8Mode;
    }

    public boolean isConvertParquet() {
        return convertParquet;
    }

    public void setConvertParquet(boolean convertParquet) {
        this.convertParquet = convertParquet;
    }
}
