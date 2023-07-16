package com.jnj.adf.springxd.export.parquet.reader;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.springxd.export.parquet.json.JsonRecordFormatter;
import com.jnj.adf.springxd.export.parquet.reader.bean.SimpleReadSupport;
import com.jnj.adf.springxd.export.parquet.reader.bean.SimpleRecord;
import com.jnj.adf.xd.mutifiles.large.util.JobStepFileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetUtil;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.drools.core.io.impl.FileSystemResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.NonTransientFlatFileException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ADFParquetReader implements ItemStreamReader<JsonObject> {
    private static final Logger logger = LoggerFactory.getLogger(ADFParquetReader.class);

    public static final Configuration configuration = new Configuration();

    private String resource;
    private String filter = "";

    private ParquetReader<SimpleRecord> reader;
    private JsonRecordFormatter.JsonGroupFormatter formatter;
    private int lineCount = 0;
    private String format;
    boolean convertParquet = true;
    private AtomicBoolean hasSent = new AtomicBoolean(false);

    public void open(ExecutionContext executionContext) {
        if (convertParquet()) {
            return;
        }

        try {
            FileSystemResource fResource = new FileSystemResource(resource);

            Path inFile = new Path(fResource.getURL().toString());

            Filter parquetFilter = ParquetUtil.makeFilter(filter);
            reader = ParquetReader.builder(new SimpleReadSupport(), inFile).withFilter(parquetFilter).build();
            ParquetMetadata metadata = ParquetFileReader.readFooter(new Configuration(), inFile, NO_FILTER);
            formatter = JsonRecordFormatter.fromSchema(metadata.getFileMetaData().getSchema());
        } catch (Exception e) {
            throw new ItemStreamException("Can not open " + resource, e);
        }

    }

    private boolean convertParquet() {
        return StringUtils.equalsIgnoreCase(format, JobStepFileUtil.FileFormat.PARQUET.name())
                || !convertParquet;
    }

    @Override
    public JsonObject read() throws Exception {
        if (convertParquet()) {
            if (!hasSent.get()) {
                hasSent.set(true);
                return JsonObject.create();
            }
            return null;
        }

        SimpleRecord value = readLine();
        if (value == null) {
            return null;
        } else {
            return formatter.makeRecord(value);
        }
    }

    public void close() {
        logger.warn("Totally read {} from {}.", lineCount, new File(resource).getAbsolutePath());
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new ItemStreamException(e);
            }
        }
    }

    private SimpleRecord readLine() {
        if (reader == null) {
            throw new ReaderNotOpenException("Reader must be open before it can be read.");
        }
        SimpleRecord line = null;
        try {
            line = this.reader.read();
            if (line == null) {
                return null;
            }
            lineCount++;
        } catch (IOException e) {
            throw new NonTransientFlatFileException("Unable to read from resource: [" + resource + "]", e, String.valueOf(line), lineCount);
        }
        return line;
    }


    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getResource() {
        return resource;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public void update(ExecutionContext executionContext) {
        //nothing need to do
    }

    public boolean isConvertParquet() {
        return convertParquet;
    }

    public void setConvertParquet(boolean convertParquet) {
        this.convertParquet = convertParquet;
    }
}
