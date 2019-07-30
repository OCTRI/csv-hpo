package org.octri.csvhpo.domain;

import java.sql.Timestamp;

/**
 * This is an Admission in the MIMIC format. Ignored/not null fields are set to nominal values.
 * @author yateam
 *
 */
public class Admission {

	private Integer rowId;
	private Integer subjectId;
	private Integer hadmId;
	private Timestamp admitTime;
	private Timestamp dischTime;
	private Timestamp deathTime;
	private String admissionType = "N/A";
	private String admissionLocation = "N/A";
	private String dischargeLocation = "N/A";
	private String insurance = "N/A";
	private String language;
	private String religion;
	private String maritalStatus;
	private String ethnicity = "N/A";
	private Timestamp edRegTime;
	private Timestamp edOutTime;
	private String diagnosis;
	private Integer hospitalExpireFlag = 0;
	private Integer hasChartEventsData = 0;

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

	public Timestamp getAdmitTime() {
		return admitTime;
	}

	public void setAdmitTime(Timestamp admitTime) {
		this.admitTime = admitTime;
	}

	public Timestamp getDischTime() {
		return dischTime;
	}

	public void setDischTime(Timestamp dischTime) {
		this.dischTime = dischTime;
	}

	public Timestamp getDeathTime() {
		return deathTime;
	}

	public void setDeathTime(Timestamp deathTime) {
		this.deathTime = deathTime;
	}

	public String getAdmissionType() {
		return admissionType;
	}

	public void setAdmissionType(String admissionType) {
		this.admissionType = admissionType;
	}

	public String getAdmissionLocation() {
		return admissionLocation;
	}

	public void setAdmissionLocation(String admissionLocation) {
		this.admissionLocation = admissionLocation;
	}

	public String getDischargeLocation() {
		return dischargeLocation;
	}

	public void setDischargeLocation(String dischargeLocation) {
		this.dischargeLocation = dischargeLocation;
	}

	public String getInsurance() {
		return insurance;
	}

	public void setInsurance(String insurance) {
		this.insurance = insurance;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getReligion() {
		return religion;
	}

	public void setReligion(String religion) {
		this.religion = religion;
	}

	public String getMaritalStatus() {
		return maritalStatus;
	}

	public void setMaritalStatus(String maritalStatus) {
		this.maritalStatus = maritalStatus;
	}

	public String getEthnicity() {
		return ethnicity;
	}

	public void setEthnicity(String ethnicity) {
		this.ethnicity = ethnicity;
	}

	public Timestamp getEdRegTime() {
		return edRegTime;
	}

	public void setEdRegTime(Timestamp edRegTime) {
		this.edRegTime = edRegTime;
	}

	public Timestamp getEdOutTime() {
		return edOutTime;
	}

	public void setEdOutTime(Timestamp edOutTime) {
		this.edOutTime = edOutTime;
	}

	public String getDiagnosis() {
		return diagnosis;
	}

	public void setDiagnosis(String diagnosis) {
		this.diagnosis = diagnosis;
	}

	public Integer getHospitalExpireFlag() {
		return hospitalExpireFlag;
	}

	public void setHospitalExpireFlag(Integer hospitalExpireFlag) {
		this.hospitalExpireFlag = hospitalExpireFlag;
	}

	public Integer getHasChartEventsData() {
		return hasChartEventsData;
	}

	public void setHasChartEventsData(Integer hasChartEventsData) {
		this.hasChartEventsData = hasChartEventsData;
	}

}
