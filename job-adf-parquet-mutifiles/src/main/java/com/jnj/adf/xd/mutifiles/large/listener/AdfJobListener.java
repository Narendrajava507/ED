package com.jnj.adf.xd.mutifiles.large.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.admin.service.JobService;
import org.springframework.batch.admin.service.SimpleJobServiceFactoryBean;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class AdfJobListener implements JobExecutionListener {
    private static final Logger logger = LoggerFactory.getLogger(AdfJobListener.class);

    private JobExecution jobExecution;

    private Map<String, String> fileMap;
    private Map<String, Closeable> writerMap;
    private AtomicInteger stepSize;

    @Autowired
    @Qualifier("dataSource")
    private DataSource dataSource;

    @Bean
    public JobService jobService() throws Exception {

        SimpleJobServiceFactoryBean factoryBean = new SimpleJobServiceFactoryBean();

        factoryBean.setDataSource(dataSource);

        factoryBean.setJobRepository(
                (JobRepository) new MapJobRepositoryFactoryBean(new ResourcelessTransactionManager()).getObject());

        factoryBean.setJobLocator(new MapJobRegistry());

        factoryBean.setJobLauncher(new SimpleJobLauncher());

        factoryBean.afterPropertiesSet();

        return factoryBean.getObject();
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        fileMap = new ConcurrentHashMap<>();
        writerMap = new ConcurrentHashMap<>();
        stepSize = new AtomicInteger(0);
        this.jobExecution = jobExecution;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
    }


    public JobExecution getJobExecution() {
        return jobExecution;
    }

    public Map<String, String> getFileMap() {
        return fileMap;
    }

    public Map<String, Closeable> getWriterMap() {
        return writerMap;
    }

    public AtomicInteger getStepSize() {
        return stepSize;
    }
}
