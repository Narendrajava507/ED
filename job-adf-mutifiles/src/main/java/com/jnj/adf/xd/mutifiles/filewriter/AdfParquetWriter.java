package com.jnj.adf.xd.mutifiles.filewriter;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.xd.mutifiles.util.JobStepFileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdfParquetWriter extends BasicWriter {
    public static final Configuration configuration = new Configuration(false);
    public static final int ONE_K = 1000;
    public static final int FIVE_K = 5 * ONE_K;
    public static final int TEN_K = 2 * FIVE_K;
    public static final int HUNDRED_K = 10 * TEN_K;
    public static final int ONE_MILLION = 10 * HUNDRED_K;
    public static final int FIXED_LEN_BYTEARRAY_SIZE = 1024;
    public static final int BLOCK_SIZE_DEFAULT = ParquetWriter.DEFAULT_BLOCK_SIZE;
    public static final int BLOCK_SIZE_256M = 256 * 1024 * 1024;
    public static final int BLOCK_SIZE_512M = 512 * 1024 * 1024;
    public static final int PAGE_SIZE_DEFAULT = ParquetWriter.DEFAULT_PAGE_SIZE;
    public static final int PAGE_SIZE_4M = 4 * 1024 * 1024;
    public static final int PAGE_SIZE_8M = 8 * 1024 * 1024;
    public static final int DICT_PAGE_SIZE = 512;
    private static final Logger logger = LoggerFactory.getLogger(AdfParquetWriter.class);
    MessageType schema;
    Path outFile;
    private String columnNames;
    private String regionName;
    private String keys;
    private Resource resouce;
    private ParquetWriter<Group> writer;
    private SimpleGroupFactory factory;
    private String parquetFileListKey;
    private String parquetVersion;
    private boolean UTF8Mode;
    private boolean dictionaryEncoding;

    AdfParquetWriter(Map<String, Object> params) {
        resouce = (Resource) params.get("resouce");
        columnNames = (String) params.get("columnNames");
        regionName = (String) params.get("regionName");
        parquetFileListKey = (String) params.get(JobStepFileUtil.FILE_LIST_KEY);
        keys = (String) params.get("keys");
        parquetVersion = (String) params.get("parquetVersion");
        UTF8Mode = (boolean) params.get("UTF8Mode");
        dictionaryEncoding = (boolean) params.get("dictionaryEncoding");
    }

    @Override
    public void close() throws IOException {
        if (writer != null)
            writer.close();
    }

    @Override
    public void open() {
        try {
            setSchema();

            outFile = new Path(resouce.getURI().toString());

            logger.info("parquet file {} is created!", resouce);

            ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outFile);
            builder.withCompressionCodec(CompressionCodecName.GZIP);
            builder.withDictionaryPageSize(DICT_PAGE_SIZE);
            builder.withConf(configuration);
            builder.withPageSize(PAGE_SIZE_DEFAULT);
            builder.withRowGroupSize(BLOCK_SIZE_DEFAULT);
            builder.withWriterVersion(getParquetVersion());
            builder.withType(schema);
            //builder.withExtraMetaData(getExtraMeta());
            builder.withValidation(false);
            builder.withDictionaryEncoding(dictionaryEncoding);
            builder.withWriteMode(Mode.OVERWRITE);
            factory = new SimpleGroupFactory(schema);
            System.out.println(factory.newGroup());
            writer = builder.build();
        } catch (Exception e) {
            logger.error("Create parqeute error.", e);
            throw new ADFException(e);
        }

    }

    private void setSchema() {
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
        schema = typeBuilder.named(getSchemeName(regionName));
    }

    private WriterVersion getParquetVersion() {
        if (StringUtils.equalsIgnoreCase(parquetVersion, "v1")) {
            return WriterVersion.PARQUET_1_0;
        } else {
            return WriterVersion.PARQUET_2_0;
        }
    }

    private Map<String, String> getExtraMeta() {
        Map<String, String> extraMeta = new HashMap<>();
        extraMeta.put("updateTime", Long.toString(System.currentTimeMillis()));
        extraMeta.put("keys", keys);
        System.out.println(extraMeta);
        return extraMeta;
    }

    private String getSchemeName(String regionName) {
        if (regionName.startsWith("/")) {
            return regionName.substring(1).replaceAll("/", "_");
        }
        return regionName.replaceAll("/", "_");
    }

    @Override
    public void write(List<? extends JsonObject> items) {
        try {
            for (JsonObject item : items) {
                Map<String, Object> map = item.toMap();
                Group row = factory.newGroup();
                String[] columns = columnNames.split(",");
                for (String col : columns) {
                    String val = String.valueOf(map.get(col));
                    if (map.get(col) == null) {
                        JobStepFileUtil.getInstance().isParquetTrue(parquetFileListKey, "does not contain col:" + col + " value:" + getErrorKeyString(map, keys, col));
                    }
                    String columnName = getRealColumnName(col);
                    row.add(columnName, val);
                }
                writer.write(row);
            }
        } catch (IOException e) {
            throw new ADFException(e);
        }
    }

    private String getErrorKeyString(Map<String, Object> map, String keys, String errorKey) {
        StringBuilder sb = new StringBuilder();
        for (String key : keys.split(",", -1)) {
            sb.append("\"").append(key).append("\":\"").append(String.valueOf(map.get(key))).append("\",\"");
        }
        sb.append(errorKey).append("\":\"").append(String.valueOf(map.get(errorKey))).append("\"");
        return sb.toString();
    }

}