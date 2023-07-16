package org.apache.parquet.hadoop;

import com.jnj.adf.grid.common.ADFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetFileWriterExt extends ParquetFileWriter {

    private Map<String, String> columnsMap;

    public ParquetFileWriterExt(Configuration configuration, MessageType schema, Path file) throws IOException {
        super(configuration, schema, file);
    }

    public ParquetFileWriterExt(Configuration configuration, MessageType schema, Path file, Mode mode) throws IOException {
        super(configuration, schema, file, mode);
    }

    public ParquetFileWriterExt(Configuration configuration, MessageType schema, Path file, Mode mode, Map<String, String> columnsMap) throws IOException {
        super(configuration, schema, file, mode);
        this.columnsMap = columnsMap;
    }

    public ParquetFileWriterExt(Configuration configuration, MessageType schema, Path file, Mode mode, long rowGroupSize, int maxPaddingSize) throws IOException {
        super(configuration, schema, file, mode, rowGroupSize, maxPaddingSize);
    }

    ParquetFileWriterExt(Configuration configuration, MessageType schema, Path file, long rowAndBlockSize, int maxPaddingSize) throws IOException {
        super(configuration, schema, file, rowAndBlockSize, maxPaddingSize);
    }

    @Override
    public void appendRowGroup(SeekableInputStream from, BlockMetaData rowGroup,
                               boolean dropColumns) throws IOException {
        startBlock(rowGroup.getRowCount());

        BlockMetaData currentBlock = getPrivateVariable("currentBlock");
        MessageType schema = getPrivateVariable("schema");
        FSDataOutputStream out = getPrivateVariable("out");

        Map<String, ColumnChunkMetaData> columnsToCopy =
                new HashMap<String, ColumnChunkMetaData>();
        for (ColumnChunkMetaData chunk : rowGroup.getColumns()) {
            columnsToCopy.put(getRealColumnName(chunk.getPath().toDotString()), chunk);
        }

        List<ColumnChunkMetaData> columnsInOrder =
                new ArrayList<ColumnChunkMetaData>();

        for (ColumnDescriptor descriptor : schema.getColumns()) {
            String path = ColumnPath.get(descriptor.getPath()).toDotString();
            ColumnChunkMetaData chunk = columnsToCopy.remove(path);
            if (chunk != null) {
                columnsInOrder.add(chunk);
            } else {
                throw new IllegalArgumentException(String.format(
                        "Missing column '%s', cannot copy row group: %s", path, rowGroup));
            }
        }

        // complain if some columns would be dropped and that's not okay
        if (!dropColumns && !columnsToCopy.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Columns cannot be copied (missing from target schema): %s",
                    Strings.join(columnsToCopy.keySet(), ", ")));
        }

        // copy the data for all chunks
        long start = -1;
        long length = 0;
        long blockCompressedSize = 0;
        for (int i = 0; i < columnsInOrder.size(); i += 1) {
            ColumnChunkMetaData chunk = columnsInOrder.get(i);

            // get this chunk's start position in the new file
            long newChunkStart = out.getPos() + length;

            // add this chunk to be copied with any previous chunks
            if (start < 0) {
                // no previous chunk included, start at this chunk's starting pos
                start = chunk.getStartingPos();
            }
            length += chunk.getTotalSize();

            if ((i + 1) == columnsInOrder.size() ||
                    columnsInOrder.get(i + 1).getStartingPos() != (start + length)) {
                // not contiguous. do the copy now.
                runMethod(getPrivateMethod("copy", SeekableInputStream.class, FSDataOutputStream.class, long.class, long.class), from, out, start, length);
                // reset to start at the next column chunk
                start = -1;
                length = 0;
            }
            currentBlock.addColumn(ColumnChunkMetaData.get(
                    ColumnPath.fromDotString(getRealColumnName(chunk.getPath().toDotString())),
                    chunk.getType(),
                    chunk.getCodec(),
                    chunk.getEncodingStats(),
                    chunk.getEncodings(),
                    chunk.getStatistics(),
                    newChunkStart,
                    newChunkStart,
                    chunk.getValueCount(),
                    chunk.getTotalSize(),
                    chunk.getTotalUncompressedSize()));

            blockCompressedSize += chunk.getTotalSize();
        }

        currentBlock.setTotalByteSize(blockCompressedSize);

        endBlock();
    }

    public String getRealColumnName(String oldCol) {
        if (columnsMap.containsKey(oldCol)) {
            return columnsMap.get(oldCol);
        }
        return oldCol;
    }

    public <T> T runMethod(Method method, Object... args) {
        try {
            boolean accessAble = method.isAccessible();
            method.setAccessible(true);
            Object invoke = method.invoke(this, args);
            method.setAccessible(accessAble);
            return (T) invoke;
        } catch (Exception e) {
            throw new ADFException(e);
        }
    }

    public Method getPrivateMethod(String name, Class<?>... parameterTypes) {
        try {
            Method declaredMethod = this.getClass().getSuperclass().getDeclaredMethod(name, parameterTypes);
            return declaredMethod;
        } catch (Exception e) {
            throw new ADFException(e);
        }
    }

    public <T> T getPrivateVariable(String name) {
        try {
            Field declaredField = this.getClass().getSuperclass().getDeclaredField(name);
            boolean accessAble = declaredField.isAccessible();
            declaredField.setAccessible(true);
            Object o = declaredField.get(this);
            declaredField.setAccessible(accessAble);
            return (T) o;
        } catch (Exception e) {
            throw new ADFException(e);
        }
    }

}
