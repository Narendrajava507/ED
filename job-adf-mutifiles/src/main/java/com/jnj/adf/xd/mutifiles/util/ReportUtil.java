package com.jnj.adf.xd.mutifiles.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;

import com.jnj.adf.xd.mutifiles.domain.DetailsValue;
import com.jnj.adf.xd.mutifiles.domain.SummarizationValue;

public class ReportUtil {
	private static final Logger logger = LoggerFactory.getLogger(ReportUtil.class);
	
	public static final String SUMMARIZATION_SHEET_NAME = "summarization";
	public static final String DETAILS_SHEET_NAME = "details";
	
	private static final ReportUtil ru = new ReportUtil();
	
	private String filePath;
	private boolean writeReport = false;
	
	private final List<DetailsValue> details = new ArrayList<>();
	private final List<SummarizationValue> summarizations = new ArrayList<>();
	
	private ReportUtil(){
		
	}
	
	public static ReportUtil getInstance(){
		return ru;
	}

	public void addDetail(DetailsValue dv){
		details.add(dv);
	}
	
	public void addDetail(String regionName, String filePath, String bucketName, long cost) {
		DetailsValue dv = new DetailsValue();
		dv.setRegion(regionName);
		dv.setPath(filePath);
		dv.setEdlPath(bucketName);
		dv.setCostMs(cost);
		dv.setSizeB(new File(filePath).length());
		addDetail(dv);
	}
	
	public void addSummarization(String regionName, String bucketName, int size, long totalSize,
			StepExecution stepExecution) {
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		String keys = executionContext.get("keys") == null ? "":(String)executionContext.get("keys");
		
		SummarizationValue sv= new SummarizationValue();
		sv.setRegion(regionName);
		sv.setEdlPath(bucketName);
		sv.setCount(size);
		sv.setSizeB(totalSize);
		long endTime = stepExecution.getEndTime() == null ? System.currentTimeMillis() : stepExecution.getEndTime().getTime();
		sv.setCostMs(endTime-stepExecution.getStartTime().getTime());
		sv.setKey(keys);
		addSummarization(sv);
	}
	

	public void addSummarization(SummarizationValue sv){
		summarizations.add(sv);
	}
	
	public void createReport(){
		try (FileOutputStream outputStream  = new FileOutputStream(filePath)){
			creatExcel(filePath);
			XSSFWorkbook workBook = new XSSFWorkbook();
			writeSummarizationSheet(createSummarizationSheet(workBook));
			writeDetailsSheet(createDetailsSheet(workBook));
			details.clear();
			workBook.write(outputStream);
		} catch (Exception e) {
			logger.error("write report error:\n",e);
		}
	}

	private void writeDetailsSheet(XSSFSheet xssfSheet) {
		for(int i=1;i<= details.size();i++){
			DetailsValue dv = details.get(i-1);
			XSSFRow row = xssfSheet.createRow(i);
			int rowIndex =0;
			row.createCell(rowIndex++).setCellValue(dv.getRegion());
			row.createCell(rowIndex++).setCellValue(dv.getPath());
			row.createCell(rowIndex++).setCellValue(dv.getEdlPath());
			row.createCell(rowIndex++).setCellValue(dv.getSizeB());
			row.createCell(rowIndex++).setCellValue(dv.getSizeM());
			row.createCell(rowIndex++).setCellValue(dv.getCostMs());
			row.createCell(rowIndex).setCellValue(dv.getCostMin());
		}
	}

	private XSSFSheet createDetailsSheet(XSSFWorkbook workBook) {
		return createSheetTitle(workBook, DETAILS_SHEET_NAME,DetailsValue.DETAILS_COLUMNS.split(","));
	}

	private void writeSummarizationSheet(XSSFSheet xssfSheet) {
		int i=1;
		for(SummarizationValue sv : summarizations){
			XSSFRow row = xssfSheet.createRow(i++);
			int rowIndex = 0;
			row.createCell(rowIndex++).setCellValue(sv.getRegion());
			row.createCell(rowIndex++).setCellValue(sv.getEdlPath());
			row.createCell(rowIndex++).setCellValue(sv.getReadSize());
			row.createCell(rowIndex++).setCellValue(sv.getCount());
			row.createCell(rowIndex++).setCellValue(sv.getSizeB());
			row.createCell(rowIndex++).setCellValue(sv.getSizeM());
			row.createCell(rowIndex++).setCellValue(sv.getCostMs());
			row.createCell(rowIndex++).setCellValue(sv.getCostMin());
			row.createCell(rowIndex).setCellValue(sv.getKey());
		}
	}

	private void creatExcel(String filePath) throws IOException {
		File file = new File(filePath);
		if(!file.getParentFile().exists()){
			file.getParentFile().mkdirs();
		}
		file.deleteOnExit();
		if(!file.createNewFile()){
			throw new IOException("Can not create excel file: "+file.getAbsolutePath()+".");
		}
	}
	
	private XSSFSheet createSummarizationSheet(XSSFWorkbook workBook){
		return createSheetTitle(workBook, SUMMARIZATION_SHEET_NAME,SummarizationValue.SUMMARIZATION_COLUMNS.split(","));
	}
	
	private XSSFSheet createSheetTitle(XSSFWorkbook workBook,String sheetName,String[] title){
		XSSFSheet sheet = workBook.createSheet(sheetName);
		if(!sheet.isSelected()) sheet.setSelected(true);
		XSSFRow titleRow = sheet.createRow(0);
		for(int i =0;i< title.length;i++){
			XSSFCell titleCell = titleRow.createCell(i);
			titleCell.setCellValue(title[i]);
		}
		return sheet;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
		this.writeReport = true;
	}

	public boolean getWriteReport() {
		return writeReport;
	}
}
