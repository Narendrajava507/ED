package com.jnj.adf.springxd.grid.delete;


import com.jnj.adf.grid.auth.AuthService;
import com.jnj.adf.grid.auth.session.AuthInternalSession;
import com.jnj.adf.grid.connect.ADFClient;
//import com.jnj.adf.grid.data.IBizExt;
import com.jnj.adf.client.api.IBiz;
import com.jnj.adf.grid.manager.GridManager;
import com.jnj.adf.grid.support.context.ADFContext;
import com.jnj.adf.grid.support.system.ADFConfigHelper;
import com.jnj.adf.grid.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;

import java.util.List;

@com.jnj.adf.config.annotations.EnableADF
public class AdfDataDeleteTasklet implements Tasklet {

    private String path;
    private String filter;
    private boolean realDelete;
    private boolean byLucene;
    private boolean byExpire;
    private String deleteDays;

    private int bigTableSize;
    private int batchSize;
    private long timeoutMs;

    private String gridName;
    private String username;
    private String password;
    private String namingServer;
    private String locators;

    private IDataDeleteService irs;
    private IQueueService iqs;

    public AdfDataDeleteTasklet() {
        //nothing to do
    }

    public RepeatStatus execute(StepContribution contribution,
                                ChunkContext chunkContext) throws Exception {
        checkConnectGrid();
        String[] regionNames = path.split(";", -1);
        String[] filters = filter.split(";", -1);
        String[] deleteDayss = deleteDays.split(";", -1);

        if (irs == null) {
            //irs = IBizExt
        	irs = IBiz
                    .lookup(IDataDeleteService.class);
        }
        if (iqs == null) {
            //iqs = IBizExt
            iqs = IBiz		
                    .lookup(IQueueService.class);
        }
        int i = 0;
        int data = 0;
        StringBuilder resultString = new StringBuilder("delete result:\r\n");
        for (String regionName : regionNames) {
            if (StringUtils.isNotBlank((deleteDayss[i]))) {
                filters[i] = "_UPT_<=" + DateUtil.addDay(System.currentTimeMillis(), -1 * Integer.valueOf(deleteDayss[i]));
            }
            if (StringUtils.isBlank(filters[i]) && realDelete) {
                iqs.disableQueue(regionName, true);
            }
            List<Integer> resultList = irs.deleteDeltaData(regionName, filters[i], realDelete, byLucene, byExpire, batchSize, timeoutMs);
            if (StringUtils.isBlank(filters[i]) && realDelete) {
                iqs.disableQueue(regionName, false);
            }
            data = 0;
            for (int result : resultList) {
                data = data + result;
            }
            resultString.append(regionName).append(" : ").append(data).append("\r\n");
            i++;
        }
        contribution.setExitStatus(ExitStatus.COMPLETED.addExitDescription(resultString.toString()));

        return RepeatStatus.FINISHED;
    }

    private void checkConnectGrid() {
        Assert.notNull(namingServer, "ADF naming servers are required.");
        Assert.notNull(username, "A username is required.");
        Assert.notNull(password, "A password is required.");
        Assert.notNull(gridName, "A grid name is required.");
        Assert.notNull(path, "A path name is required.");
        //Assert.notNull(locators, "ADF locators are required.");

        //ADFConfigHelper.setProperty(ADFConfigHelper.ITEMS.LOCATORS, locators);
        ADFConfigHelper.setProperty(ADFConfigHelper.ITEMS.NAMING_SERVER,
                namingServer);
        boolean isConnected = connect();
        Assert.isTrue(isConnected, "Connect to grid failed, namingserver are: "
                + namingServer + ".");
        AuthInternalSession tlSession = login();
        Assert.notNull(tlSession, "Login to grid failed, username is: "
                + username + ".");
    }

    private boolean connect() {
		/*
		 * boolean isConnected = GridManager.getInstance().isConnect(gridName); if
		 * (!isConnected) { isConnected = ADFClient.connectGridById(gridName); } return
		 * isConnected;
		 */
		 return ADFClient.connectGridById(gridName); 
    }

    private AuthInternalSession login() {
        AuthService authService = ADFContext.getBean(AuthService.class);
        return authService.login(username, password);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getGridName() {
        return gridName;
    }

    public void setGridName(String gridName) {
        this.gridName = gridName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNamingServer() {
        return namingServer;
    }

    public void setNamingServer(String namingServer) {
        this.namingServer = namingServer;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public boolean isByLucene() {
        return byLucene;
    }

    public void setByLucene(boolean byLucene) {
        this.byLucene = byLucene;
    }

    public boolean isRealDelete() {
        return realDelete;
    }

    public void setRealDelete(boolean realDelete) {
        this.realDelete = realDelete;
    }

    public int getBigTableSize() {
        return bigTableSize;
    }

    public void setBigTableSize(int bigTableSize) {
        this.bigTableSize = bigTableSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public String getLocators() {
        return locators;
    }

    public void setLocators(String locators) {
        this.locators = locators;
    }

    public boolean isByExpire() {
        return byExpire;
    }

    public void setByExpire(boolean byExpire) {
        this.byExpire = byExpire;
    }

    public String getDeleteDays() {
        return deleteDays;
    }

    public void setDeleteDays(String deleteDays) {
        this.deleteDays = deleteDays;
    }

}
