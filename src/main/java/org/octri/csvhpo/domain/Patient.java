package org.octri.csvhpo.domain;

import java.sql.Timestamp;

/**
 * This is a Patient in the MIMIC format.
 * 
 * @author yateam
 *
 */
public class Patient {

	private Integer rowId;
	private Integer subjectId;
	private String gender;
	private String dob;
	private Timestamp dod;
	private Timestamp dodHosp;
	private Timestamp dodSsn;
	private Integer expireFlag = 0;

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

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getDob() {
		return dob;
	}

	public void setDob(String dob) {
		this.dob = dob;
	}

	public Timestamp getDod() {
		return dod;
	}

	public void setDod(Timestamp dod) {
		this.dod = dod;
	}

	public Timestamp getDodHosp() {
		return dodHosp;
	}

	public void setDodHosp(Timestamp dodHosp) {
		this.dodHosp = dodHosp;
	}

	public Timestamp getDodSsn() {
		return dodSsn;
	}

	public void setDodSsn(Timestamp dodSsn) {
		this.dodSsn = dodSsn;
	}

	public Integer getExpireFlag() {
		return expireFlag;
	}

	public void setExpireFlag(Integer expireFlag) {
		this.expireFlag = expireFlag;
	}

}
