package com.jnj.adf.xd.mutifiles.large.filewriter;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import org.apache.parquet.hadoop.ParquetUtil;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AdfParquetWriter extends BasicWriter {
    private static final Logger logger = LoggerFactory.getLogger(AdfParquetWriter.class);

    private String columnNames;
    private String regionName;
    private String keys;
    private Resource resouce;

    private String queryString;
    private String parquetPath;
    private boolean UTF8Mode;

    AdfParquetWriter(ExecutionContext executionContext, Map<String, Object> params) {
        resouce = (Resource) params.get("resouce");
        columnNames = (String) params.get("columnNames");
        regionName = (String) params.get("regionName");
        keys = (String) params.get("keys");
        queryString = executionContext.getString("queryString");
        parquetPath = executionContext.getString("parquetPath");
        UTF8Mode = (boolean) params.get("UTF8Mode");
    }

    @Override
    public void close() throws IOException {
        //nothing to do
    }

    @Override
    public void open() {
        try {
            Types.MessageTypeBuilder typeBuilder = Types.buildMessage();
            String[] columns = columnNames.split(",");
            for (String col : columns) {
                String columnName = getRealColumnName(col);
                if (UTF8Mode) {
                    Type t0 = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, columnName, OriginalType.UTF8);
                    typeBuilder.addField(t0);
                } else {
                    Type t0 = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, columnName);
                    typeBuilder.addField(t0);
                }
            }
            MessageType schema = typeBuilder.named(getSchemeName(regionName));

            logger.warn("Starting convert {} to {} .", parquetPath, resouce.getFile().getAbsoluteFile());
            ParquetUtil.convertParquetByColumns(parquetPath, resouce.getFile().getAbsolutePath(), ParquetUtil.makeFilter(queryString), schema, keys, getColumnsMap());
        } catch (Exception e) {
            throw new ADFException(e);
        }

    }

    private String getSchemeName(String regionName) {
        if (regionName.startsWith("/")) {
            return regionName.substring(1).replaceAll("/", "_");
        }
        return regionName.replaceAll("/", "_");
    }


    @Override
    public void write(List<? extends JsonObject> items) {
        //nothing to do
    }
}
