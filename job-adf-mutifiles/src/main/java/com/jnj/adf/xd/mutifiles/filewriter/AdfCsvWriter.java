package com.jnj.adf.xd.mutifiles.filewriter;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.grid.utils.LogUtil;
import com.jnj.adf.xd.mutifiles.util.CsvConvertUtil;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AdfCsvWriter extends BasicWriter {

    private Resource resouce;
    private String columnNames;
    private CsvConvertUtil csvWriter = null;

    AdfCsvWriter(Map<String, Object> params) {
        resouce = (Resource) params.get("resouce");
        columnNames = (String) params.get("columnNames");
    }


    @Override
    public void close() throws IOException {
        if (csvWriter != null)
            csvWriter.close();
    }

    @Override
    public void open() {
        try {
            String[] head = columnNames.split(",");
            csvWriter = new CsvConvertUtil(resouce.getFile().getAbsolutePath());
            csvWriter.writeHead(getRealColumnNames(head));
            LogUtil.getCoreLog().info("csv file " + resouce.getURI().toString() + "is created!");
        } catch (Exception e) {
            throw new ADFException(e);
        }

    }

    @Override
    public void write(List<? extends JsonObject> items) {
        try {
            for (JsonObject item : items) {
                csvWriter.writeNext(transformColumn(item));
            }
        } catch (Exception e) {
            throw new ADFException(e);
        }
    }
}
