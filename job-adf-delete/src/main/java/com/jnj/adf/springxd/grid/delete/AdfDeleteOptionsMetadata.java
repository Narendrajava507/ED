package com.jnj.adf.springxd.grid.delete;

import org.springframework.xd.module.options.spi.ModuleOption;

public class AdfDeleteOptionsMetadata {

	 private String path;
	 private String gridName = "";
	 private String username = "";
	 private String password = "";
	 private String namingServer = "";
	 private String filter = "";
	 private boolean realDelete = true;
	 private boolean byLucene = true;
	 private int batchSize = 1000;
	 private long timeoutMs = 1000 * 60 * 10L;
	 private boolean byExpire = false;
	 private String deleteDays = "";

	public String getPath() {
		return path;
	}

	@ModuleOption(value = "tables", hidden = true)
	public void setPath(String path) {
		this.path = path;
	}

	public String getGridName() {
		return gridName;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setGridName(String gridName) {
		this.gridName = gridName;
	}

	public String getUsername() {
		return username;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setPassword(String password) {
		this.password = password;
	}

	public String getNamingServer() {
		return namingServer;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setNamingServer(String namingServer) {
		this.namingServer = namingServer;
	}

	public String getFilter() {
		return filter;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setFilter(String filter) {
		this.filter = filter;
	}


	public boolean isRealDelete() {
		return realDelete;
	}

	@ModuleOption(value = "tables", hidden = true)
	public void setRealDelete(boolean realDelete) {
		this.realDelete = realDelete;
	}

	public boolean isByLucene() {
		return byLucene;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setByLucene(boolean byLucene) {
		this.byLucene = byLucene;
	}

	public int getBatchSize() {
		return batchSize;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public long getTimeoutMs() {
		return timeoutMs;
	}
	@ModuleOption(value = "tables", hidden = true)
	public void setTimeoutMs(long timeoutMs) {
		this.timeoutMs = timeoutMs;
	}

	public boolean isByExpire() {
		return byExpire;
	}

	@ModuleOption(value = "tables", hidden = true)
	public void setByExpire(boolean byExpire) {
		this.byExpire = byExpire;
	}

	public String getDeleteDays() {
		return deleteDays;
	}

	@ModuleOption(value = "tables", hidden = true)
	public void setDeleteDays(String deleteDays) {
		this.deleteDays = deleteDays;
	}

}
