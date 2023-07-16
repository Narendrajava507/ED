package com.jnj.adf.xd.mutifiles.large.domain;

public class SummarizationValue {
    public static final String SUMMARIZATION_COLUMNS = "Region,EDLPath,RegionSize,ParquetCount,ParuqetSize(btyes),ParuqetSize(MB),Cost(ms),Cost(min),key";

    private String region;
    private String edlPath;
    private int readSize;
    private int count;
    private long sizeB;
    private double sizeM;
    private long costMs;
    private double costMin;
    private String key;

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getEdlPath() {
        return edlPath;
    }

    public void setEdlPath(String edlPath) {
        this.edlPath = edlPath;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getSizeB() {
        return sizeB;
    }

    public void setSizeB(long sizeB) {
        this.sizeB = sizeB;
        this.sizeM = sizeB * 1.0 / 1024 / 1024;
    }

    public double getSizeM() {
        return sizeM;
    }

    public long getCostMs() {
        return costMs;
    }

    public void setCostMs(long costMs) {
        this.costMs = costMs;
        this.costMin = costMs * 1.0 / 1000 / 60;
    }

    public double getCostMin() {
        return costMin;
    }

    public int getReadSize() {
        return readSize;
    }

    public void setReadSize(int readSize) {
        this.readSize = readSize;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SummarizationValue))
            return false;
        SummarizationValue sv = (SummarizationValue) obj;

        if (this.region == null || sv.getRegion() == null)
            return false;

        return this.region.equals(sv.getRegion());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
