package com.jnj.adf.xd.mutifiles.filewriter;

import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.xd.mutifiles.util.JobStepFileUtil;

import java.util.Map;


public class AdfMutiFilesWriterFactory {

    private AdfMutiFilesWriterFactory() {
        throw new IllegalStateException("Utility class");
    }

    public static BasicWriter getWriter(String format, Map<String, Object> params) {
        if (JobStepFileUtil.FileFormat.AVRO.equalToMe(format)) {
            return new AdfAvroWriter( params);
        }
        if (JobStepFileUtil.FileFormat.CSV.equalToMe(format)) {
            return new AdfCsvWriter( params);
        }
        if (JobStepFileUtil.FileFormat.PARQUET.equalToMe(format)) {
            return new AdfParquetWriter(params);
        }
        if (JobStepFileUtil.FileFormat.JSON.equalToMe(format)) {
            return new AdfJsonWriter( params);
        }
        if (JobStepFileUtil.FileFormat.OPTYMYZE.equalToMe(format)) {
            return new AdfOptymyzeWriter(params);
        }
        throw new ADFException("Cant't support this format:" + format);
    }
}
