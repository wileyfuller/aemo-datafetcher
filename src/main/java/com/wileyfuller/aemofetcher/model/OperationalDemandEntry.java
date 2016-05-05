package com.wileyfuller.aemofetcher.model;

import java.util.Date;

/**
 * Created by wiley on 29/04/2016.
 */
public class OperationalDemandEntry {
    private String regionId;
    private Date intervalDateTime;
    private int demand;
    private DemandType type;

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public Date getIntervalDateTime() {
        return intervalDateTime;
    }

    public void setIntervalDateTime(Date intervalDateTime) {
        this.intervalDateTime = intervalDateTime;
    }

    public int getDemand() {
        return demand;
    }

    public void setDemand(int demand) {
        this.demand = demand;
    }

    public DemandType getType() {
        return type;
    }

    public void setType(DemandType type) {
        this.type = type;
    }

    public enum DemandType {
        ACTUAL, FORECAST
    }
}
