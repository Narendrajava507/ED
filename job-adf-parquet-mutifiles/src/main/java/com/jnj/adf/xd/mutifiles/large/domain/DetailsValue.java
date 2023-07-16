package com.jnj.adf.xd.mutifiles.large.domain;

public class DetailsValue {
    public static final String DETAILS_COLUMNS = "Region,ParquetPath,EDLPath,ParuqetSize(btyes),ParuqetSize(MB),Cost(ms),Cost(min)";

    private String region;
    private String path;
    private String edlPath;
    private long sizeB;
    private double sizeM;
    private long costMs;
    private double costMin;

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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DetailsValue))
            return false;
        DetailsValue dv = (DetailsValue) obj;

        if (this.region == null || dv.getRegion() == null || this.path == null || dv.path == null)
            return false;

        return this.region.equals(dv.getRegion()) && this.path.equals(dv.getPath());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
