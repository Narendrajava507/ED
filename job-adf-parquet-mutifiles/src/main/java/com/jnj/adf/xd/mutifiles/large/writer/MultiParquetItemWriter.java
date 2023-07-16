package com.jnj.adf.xd.mutifiles.large.writer;

import com.jnj.adf.grid.common.ADFException;
import com.jnj.adf.springxd.job.util.JobStepFileUtil;
import com.jnj.adf.xd.mutifiles.large.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.batch.item.file.SimpleResourceSuffixCreator;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MultiParquetItemWriter<T> extends AbstractItemStreamItemWriter<T> {
    private static final Logger logger = LoggerFactory.getLogger(MultiParquetItemWriter.class);

    private static final String RESOURCE_INDEX_KEY = "resource.index";

    private static final String CURRENT_RESOURCE_ITEM_COUNT = "resource.item.count";

    private Resource resource;

    private ResourceAwareItemWriterItemStream<? super T> delegate;

    private int itemCountLimitPerResource = Integer.MAX_VALUE;

    private AtomicInteger currentResourceItemCount = new AtomicInteger(0);

    private AtomicInteger resourceIndex = new AtomicInteger(1);

    private ResourceSuffixCreator suffixCreator = new SimpleResourceSuffixCreator();

    private AtomicBoolean saveState = new AtomicBoolean(true);

    private AtomicBoolean opened = new AtomicBoolean(false);

    private ReentrantLock writeLock = new ReentrantLock();

    private boolean writeParquet = true;

    private String regionName;

    private String format;

    private ExecutionContext executionContext;

    public MultiParquetItemWriter() {
        this.setExecutionContextName(ClassUtils.getShortName(MultiParquetItemWriter.class));
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        if (!isWriteParquet())
            return;
        try {
            writeLock.lock();

            if (!opened.get()) {
                opened.set(true);
                File file = setResourceToDelegate();
                // create only if write is called
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                Files.deleteIfExists(file.toPath());
                if(file.createNewFile() && file.canWrite()){
                    delegate.open(executionContext);
                }else{
                    throw new ADFException("Output resource " + file.getAbsolutePath() + " must be writable");
                }

            }
            delegate.write(items);
            currentResourceItemCount.getAndAdd(items.size());

            if (currentResourceItemCount.get() >= itemCountLimitPerResource) {
                delegate.close();
                resourceIndex.getAndIncrement();
                currentResourceItemCount = new AtomicInteger(0);
                setResourceToDelegate();
                opened.set(false);
            }
        } catch (Exception e) {
            logger.error("Write error.", e);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Allows customization of the suffix of the created resources based on the
     * index.
     */
    public void setResourceSuffixCreator(ResourceSuffixCreator suffixCreator) {
        this.suffixCreator = suffixCreator;
    }

    /**
     * After this limit is exceeded the next chunk will be written into newly
     * created resource.
     */
    public void setItemCountLimitPerResource(int itemCountLimitPerResource) {
        this.itemCountLimitPerResource = itemCountLimitPerResource;
    }

    /**
     * Delegate used for actual writing of the output.
     */
    public void setDelegate(ResourceAwareItemWriterItemStream<? super T> delegate) {
        this.delegate = delegate;
    }

    /**
     * Prototype for output resources. Actual output files will be created in
     * the same directory and use the same name as this prototype with appended
     * suffix (according to
     * {@link #setResourceSuffixCreator(ResourceSuffixCreator)}.
     */
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public void setSaveState(boolean saveState) {
        this.saveState.set(saveState);
    }

    public boolean isWriteParquet() {
        return writeParquet;
    }

    public void setWriteParquet(boolean writeParquet) {
        this.writeParquet = writeParquet;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    @Override
    public void close() {
        super.close();
        if (!isWriteParquet())
            return;
        writeLock.lock();
        resourceIndex = new AtomicInteger(1);
        currentResourceItemCount = new AtomicInteger(0);
        if (opened.get()) {
            delegate.close();
        }
        writeLock.unlock();
        this.executionContext = null;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);
        this.executionContext = executionContext;

        if (!isWriteParquet())
            return;
        writeLock.lock();
        resourceIndex = new AtomicInteger(executionContext.getInt(getExecutionContextKey(RESOURCE_INDEX_KEY), 1));
        currentResourceItemCount = new AtomicInteger(
                executionContext.getInt(getExecutionContextKey(CURRENT_RESOURCE_ITEM_COUNT), 0));

        try {
            setResourceToDelegate();
        } catch (IOException e) {
            writeLock.unlock();
            throw new ItemStreamException("Couldn't assign resource", e);
        }

        if (executionContext.containsKey(getExecutionContextKey(CURRENT_RESOURCE_ITEM_COUNT))) {
            // It's a restart
            delegate.open(executionContext);
        } else {
            opened.set(false);
        }
        writeLock.unlock();
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (!isWriteParquet())
            return;
        super.update(executionContext);
        if (saveState.get()) {
            if (opened.get()) {
                delegate.update(executionContext);
            }
            executionContext.putInt(getExecutionContextKey(CURRENT_RESOURCE_ITEM_COUNT),
                    currentResourceItemCount.get());
            executionContext.putInt(getExecutionContextKey(RESOURCE_INDEX_KEY), resourceIndex.get());
        }
    }

    /**
     * Create output resource (if necessary) and point the delegate to it.
     */
    private File setResourceToDelegate() throws IOException {
        String path = resource.getFile().getAbsolutePath() + File.separator
                + CommonUtils.getRegionPath(regionName, false) + File.separator
                + suffixCreator.getSuffix(resourceIndex.get());
        File file = new File(path);
        delegate.setResource(new FileSystemResource(file));
        return file;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        if (format != null && JobStepFileUtil.FileFormat.contains(format)) {
            this.writeParquet = true;
        } else {
            this.writeParquet = false;
        }
    }
}
