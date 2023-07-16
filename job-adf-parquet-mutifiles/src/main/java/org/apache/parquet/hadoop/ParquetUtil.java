package org.apache.parquet.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ParquetUtil {

    private static final Logger logger = LoggerFactory.getLogger(ParquetUtil.class);

    private ParquetUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static List<String> getColumnsFromParquet(String parquetPath) throws IOException {
        Path inPath = new Path(parquetPath);

        Configuration conf = new Configuration();

        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);

        List<String> columnNames = new ArrayList<>();

        metadata.getFileMetaData().getSchema().getColumns().forEach(columnDescriptor -> {
            for (String colPath : columnDescriptor.getPath()) {
                columnNames.add(colPath);
            }
        });

        return columnNames;
    }

    public static long getParquetRowCount(String parquetPath) throws IOException {
        Path inPath = new Path(parquetPath);
        Configuration conf = new Configuration();

        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);

        return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
    }


    public static long convertParquetByColumns(String parquetPath, String outFilePath, FilterCompat.Filter filter, MessageType outSchema, String keys, Map<String, String> columnsMap) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Path inPath = new Path(parquetPath);
        Path outPath = new Path(outFilePath);

        Configuration conf = new Configuration();

        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);

        Map<String, String> customSchema = new HashMap<>();
        if (StringUtils.isNotBlank(keys)) {
            customSchema.put("keys", keys);
        }

        long size = 0;

        try (ParquetFileReader reader = new ParquetFileReader(conf, metadata.getFileMetaData(), inPath, metadata.getBlocks(), outSchema.getColumns())) {

            Method filterRowGroups = ParquetFileReader.class.getDeclaredMethod("filterRowGroups", FilterCompat.Filter.class);
            boolean accessAble = filterRowGroups.isAccessible();
            filterRowGroups.setAccessible(true);
            filterRowGroups.invoke(reader, filter);
            filterRowGroups.setAccessible(accessAble);

            size = reader.getRecordCount();
            Configuration configuration = new Configuration(false);
            ParquetFileWriterExt writer = new ParquetFileWriterExt(configuration, outSchema,
                    outPath, ParquetFileWriter.Mode.OVERWRITE, columnsMap);
            writer.start();
            reader.appendTo(writer);
            writer.end(customSchema);
        } catch (Exception e) {
            logger.error("Convert parqeut fiels error.", e);
            throw e;
        }
        return size;
    }

    public static long convertParquetByColumns(String parquetPath, String outFilePath, FilterCompat.Filter filter, MessageType outSchema, String keys) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Path inPath = new Path(parquetPath);
        Path outPath = new Path(outFilePath);

        Configuration conf = new Configuration();

        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);

        Map<String, String> customSchema = new HashMap<>();
        if (StringUtils.isNotBlank(keys)) {
            customSchema.put("keys", keys);
        }

        long size = 0;

        try (ParquetFileReader reader = new ParquetFileReader(conf, metadata.getFileMetaData(), inPath, metadata.getBlocks(), outSchema.getColumns())) {

            Method filterRowGroups = ParquetFileReader.class.getDeclaredMethod("filterRowGroups", FilterCompat.Filter.class);
            boolean accessAble = filterRowGroups.isAccessible();
            filterRowGroups.setAccessible(true);
            filterRowGroups.invoke(reader, filter);
            filterRowGroups.setAccessible(accessAble);

            size = reader.getRecordCount();
            Configuration configuration = new Configuration(false);
            ParquetFileWriter writer = new ParquetFileWriter(configuration, outSchema,
                    outPath, ParquetFileWriter.Mode.OVERWRITE);
            writer.start();
            reader.appendTo(writer);
            writer.end(customSchema);
        } catch (Exception e) {
            logger.error("Convert parqeut fiels error.", e);
            throw e;
        }
        return size;
    }

    public static FilterCompat.Filter makeFilter(String filter) {
        if (StringUtils.isEmpty(filter) || StringUtils.equals("*:*", filter)) {
            return FilterCompat.NOOP;
        } else {
            return FilterCompat.get(handleFilter(filter));
        }
    }


    private static FilterPredicate handleFilter(String filterString) {
        String[] cmds = filterString.split(" ");
        for (String cmd : cmds) {
            if (cmd.equalsIgnoreCase("and")) {
                String leftStr = filterString.substring(0, filterString.indexOf(" and "));
                FilterPredicate fpl = handleFilter(leftStr);
                String rightStr = filterString.substring(filterString.indexOf(" and ") + 5, filterString.length());
                FilterPredicate fpr = handleFilter(rightStr);
                return FilterApi.and(fpl, fpr);
            } else if (cmd.equalsIgnoreCase("or")) {
                String leftStr = filterString.substring(0, filterString.indexOf(" or "));
                FilterPredicate fpl = handleFilter(leftStr);
                String rightStr = filterString.substring(filterString.indexOf(" or ") + 4, filterString.length());
                FilterPredicate fpr = handleFilter(rightStr);
                return FilterApi.or(fpl, fpr);
            }
        }
        return handleSingleExpress(filterString);
    }

    private static FilterPredicate handleSingleExpress(String filterString) {
        String[] cmds = filterString.split(" ");
        for (String cmd : cmds) {
            if (cmd.equalsIgnoreCase(">=")) {
                String colName = filterString.substring(0, filterString.indexOf(" >= "));
                String colVal = filterString.substring(filterString.indexOf(" >= ") + 4, filterString.length());
                Operators.BinaryColumn column = FilterApi.binaryColumn(colName.replace("'", ""));
                return FilterApi.gtEq(column, Binary.fromString(colVal.replace("'", "")));
            } else if (cmd.equalsIgnoreCase(">")) {
                String colName = filterString.substring(0, filterString.indexOf(" > "));
                String colVal = filterString.substring(filterString.indexOf(" > ") + 3, filterString.length());
                Operators.BinaryColumn column = FilterApi.binaryColumn(colName.replace("'", ""));
                return FilterApi.gt(column, Binary.fromString(colVal.replace("'", "")));
            } else if (cmd.equalsIgnoreCase("<=")) {
                String colName = filterString.substring(0, filterString.indexOf(" <= "));
                String colVal = filterString.substring(filterString.indexOf(" <= ") + 4, filterString.length());
                Operators.BinaryColumn column = FilterApi.binaryColumn(colName.replace("'", ""));
                return FilterApi.ltEq(column, Binary.fromString(colVal.replace("'", "")));
            } else if (cmd.equalsIgnoreCase("<")) {
                String colName = filterString.substring(0, filterString.indexOf(" < "));
                String colVal = filterString.substring(filterString.indexOf(" < ") + 3, filterString.length());
                Operators.BinaryColumn column = FilterApi.binaryColumn(colName.replace("'", ""));
                return FilterApi.lt(column, Binary.fromString(colVal.replace("'", "")));
            } else if (cmd.equalsIgnoreCase("=")) {
                String colName = filterString.substring(0, filterString.indexOf(" = "));
                String colVal = filterString.substring(filterString.indexOf(" = ") + 3, filterString.length());
                Operators.BinaryColumn column = FilterApi.binaryColumn(colName.replace("'", ""));
                return FilterApi.eq(column, Binary.fromString(colVal.replace("'", "")));
            }
        }
        return null;
    }
}
