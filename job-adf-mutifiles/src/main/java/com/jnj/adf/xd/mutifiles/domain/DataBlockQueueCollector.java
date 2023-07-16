package com.jnj.adf.xd.mutifiles.domain;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;

//import com.gemstone.gemfire.DataSerializer;
import org.apache.geode.DataSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.Version;*/
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.Version;

import com.jnj.adf.client.api.JsonObject;
import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.xd.mutifiles.remoteservice.SimpleKryoSerializer;
import com.jnj.adf.xd.mutifiles.util.JobStepFileUtil;

public class DataBlockQueueCollector  implements ResultCollector<Object, BlockingQueue<Object>> {
    private static final Logger logger = LoggerFactory.getLogger(DataBlockQueueCollector.class);

    private BlockingQueue<Object> results;

    private AtomicBoolean isEnd = new AtomicBoolean(false);

    private String[] columns;
    private String primaryKey;

    private AtomicLong size = new AtomicLong(0L);

    private AtomicInteger recieveDataCount = new AtomicInteger(0);

    private boolean jobRuning = true;

    public DataBlockQueueCollector(int queueSize) {
        results = new ArrayBlockingQueue<>(queueSize);
    }

    public DataBlockQueueCollector(int queueSize, String primaryKey) {
        results = new ArrayBlockingQueue<>(queueSize);
        this.primaryKey = primaryKey;
    }

    @Override
    public BlockingQueue<Object> getResult() {
        return results;
    }

    @Override
    public BlockingQueue<Object> getResult(long paramLong, TimeUnit paramTimeUnit)
            throws InterruptedException {
        return results;
    }

    @SuppressWarnings("resource")
    @Override
    public void addResult(DistributedMember paramDistributedMember, Object result) {
        if (result instanceof Throwable || !jobRuning) {
            endResults();
            if (!jobRuning) {
                throw new ADFException("Collector run error since job is colsed!");
            } else {
                throw new ADFException((Throwable) result);
            }
        } else {
            if (result instanceof byte[]) {
                byte[] chunk = (byte[]) result;
                handlerChunk(paramDistributedMember, chunk);
            }
        }
    }

    private void handlerChunk(DistributedMember paramDistributedMember, byte[] chunk) {
        if (chunk == null || chunk.length <= 1)
            return;

        try (ByteArrayDataInput input = new ByteArrayDataInput()) {
            input.initialize(chunk, Version.CURRENT);
            byte chunkType = input.readByte();
            switch (chunkType) {
                case StreamConstant.COLUMN_CHUNK:
                    putColumns(input, paramDistributedMember);
                    return;
                case StreamConstant.DATA_CHUNK:
                    putData(input, paramDistributedMember);
                    return;
                case StreamConstant.SIZE_CHUNK:
                    handlerSizeChunk(input, paramDistributedMember);
                    return;
                case StreamConstant.ERROR_CHUNK:
                    Exception e = DataSerializer.readObject(input);
                    throw new ADFException(e);
                default:
                    throw new ADFException("unknown chunk type: chunkType");
            }
        } catch (Exception e) {
            endResults();
            logger.error("receive data error", e);
            throw new ADFException(e);
        }
    }

    private void handlerSizeChunk(ByteArrayDataInput input, DistributedMember paramDistributedMember) {
        int pz = input.position();
        try {
            long sendSize = DataSerializer.readLong(input);
            size.addAndGet(sendSize);
            logger.warn("{} send {} data!", paramDistributedMember.getName(), sendSize);
        } catch (Exception e) {
            logger.error("Get server send size error. Try to use readInteger .", e);
            input.setPosition(pz);
            try {
                int sendSize = DataSerializer.readInteger(input);
                size.addAndGet(sendSize);
                logger.warn(" {} send  {}  data!", paramDistributedMember.getName(), sendSize);
            } catch (IOException e1) {
                logger.error("Get server size error.", e);
                throw new ADFException(e1);
            }

        }
    }

    private synchronized void putColumns(ByteArrayDataInput input, DistributedMember paramDistributedMember) throws IOException {
        if (columns == null) {
            columns = DataSerializer.readStringArray(input);
            if (columns == null) {
                throw new ADFException("get null columns from " + paramDistributedMember.getName());
            } else {
                logger.warn(" columns is set for {} ", paramDistributedMember.getName());
            }
        }
    }

    private void putData(ByteArrayDataInput input, DistributedMember paramDistributedMember) throws IOException, InterruptedException {

        while (input.available() > 0) {
            byte b = input.readByte();
            if (b == StreamConstant.BYTEARR_ARR_DATA) {
                byte[] valueBytes = DataSerializer.readByteArray(input);
                Object[] data = SimpleKryoSerializer.toObject(valueBytes, Object[].class);
                if (data == null) {
                    logger.error("get null data from {} ", paramDistributedMember.getName());
                    return;
                }
                if (columns != null && columns.length == data.length) {
                    JsonObject js = JsonObject.create();
                    for (int i = 0; i < columns.length; i++) {
                        js.append(columns[i], data[i]);
                    }
                    syncSendData(js.toJson(), 6);
                    recieveDataCount.incrementAndGet();
                } else {
                    throw new ADFException("columns leng not equal data length");
                }
            }
        }
    }

    private void syncSendData(String result, int maxRetryTimes) throws InterruptedException {
        int i = 0;
        boolean bool = results.offer(result);
        while (!bool) {
            i++;
            if (i >= maxRetryTimes || !jobRuning) {
                throw new ADFException("Collector run error since job is colsed!,i:" + i);
            }
            bool = results.offer(result, 10 * 1000L, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void endResults() {
        if (StringUtils.isNotBlank(this.primaryKey)) {
            JobStepFileUtil.getInstance().putServerSendSize(primaryKey, Integer.valueOf(String.valueOf(size.get())));
        }
        isEnd.set(true);
    }

    @Override
    public void clearResults() {
        results.clear();
    }

    public boolean isJobRuning() {
        return jobRuning;
    }

    public void setJobRuning(boolean jobRuning) {
        this.jobRuning = jobRuning;
    }

    public boolean isEnd() {
        return isEnd.get();
    }
}
