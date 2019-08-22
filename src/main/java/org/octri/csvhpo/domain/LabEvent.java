package org.octri.csvhpo.domain;

import java.sql.Timestamp;

/**
 * This is a LabEvent in the MIMIC format.
 * 
 * @author yateam
 *
 */
public class LabEvent {

	private Integer rowId;
	private Integer subjectId;
	private Integer hadmId;
	private Integer itemId;
	private Timestamp chartTime;
	private String value;
	private Double valueNum;
	private String valueUom;
	private String flag;
	private String refLow;
	private String refHigh;
	
	public LabEvent() {
		
	}

	public LabEvent(Integer rowId, Integer subjectId, Integer hadmId, Integer itemId, Timestamp chartTime, String value,
			Double valueNum, String valueUom, String flag, String refLow, String refHigh) {
		this.rowId = rowId;
		this.subjectId = subjectId;
		this.hadmId = hadmId;
		this.itemId = itemId;
		this.chartTime = chartTime;
		this.value = value;
		this.valueNum = valueNum;
		this.valueUom = valueUom;
		this.flag = flag;
		this.refLow = refLow;
		this.refHigh = refHigh;
	}

	public Integer getRowId() {
		return rowId;
	}

	public void setRowId(Integer rowId) {
		this.rowId = rowId;
	}

	public Integer getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(Integer subjectId) {
		this.subjectId = subjectId;
	}

	public Integer getHadmId() {
		return hadmId;
	}

	public void setHadmId(Integer hadmId) {
		this.hadmId = hadmId;
	}

	public Integer getItemId() {
		return itemId;
	}

	public void setItemId(Integer itemId) {
		this.itemId = itemId;
	}

	public Timestamp getChartTime() {
		return chartTime;
	}

	public void setChartTime(Timestamp chartTime) {
		this.chartTime = chartTime;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Double getValueNum() {
		return valueNum;
	}

	public void setValueNum(Double valueNum) {
		this.valueNum = valueNum;
	}

	public String getValueUom() {
		return valueUom;
	}

	public void setValueUom(String valueUom) {
		this.valueUom = valueUom;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public String getRefLow() {
		return refLow;
	}

	public void setRefLow(String refLow) {
		this.refLow = refLow;
	}

	public String getRefHigh() {
		return refHigh;
	}

	public void setRefHigh(String refHigh) {
		this.refHigh = refHigh;
	}

}
