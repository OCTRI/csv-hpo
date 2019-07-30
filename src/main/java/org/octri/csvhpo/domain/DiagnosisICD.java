package org.octri.csvhpo.domain;

public class DiagnosisICD {

	private Integer rowId;
	private Integer subjectId;
	private Integer hadmId;
	private Integer seqNum;
	private String icd9Code;

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

	public Integer getSeqNum() {
		return seqNum;
	}

	public void setSeqNum(Integer seqNum) {
		this.seqNum = seqNum;
	}

	public String getIcd9Code() {
		return icd9Code;
	}

	public void setIcd9Code(String icd9Code) {
		this.icd9Code = icd9Code;
	}
}
