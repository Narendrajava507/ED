package com.jnj.adf.xd.mutifiles.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class AdfJobListener implements JobExecutionListener{
private static final Logger logger = LoggerFactory.getLogger(AdfJobListener.class);

	@Override
	public void beforeJob(JobExecution jobExecution) {
		//nothing to do
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		try {
//			if(ReportUtil.getInstance().getWriteReport()){
//				ReportUtil.getInstance().createReport();
//			}
		} catch (Exception e) {
			logger.error("Do afterJob error.",e);
		}
	}
}
