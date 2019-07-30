package org.octri.csvhpo.service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.octri.csvhpo.domain.Admission;
import org.octri.csvhpo.domain.DiagnosisICD;
import org.octri.csvhpo.domain.LabEvent;
import org.octri.csvhpo.domain.LabItem;
import org.octri.csvhpo.domain.Patient;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class DatabaseService {

	private JdbcTemplate jdbcTemplate;

	private PlatformTransactionManager transactionManager;
	
	public static final String TABLE_PATIENTS = "PATIENTS";
	public static final String TABLE_ADMISSIONS = "ADMISSIONS";
	public static final String TABLE_DIAGNOSES_ICD = "DIAGNOSES_ICD";
	public static final String TABLE_D_LABITEMS = "D_LABITEMS";
	public static final String TABLE_LABEVENTS = "LABEVENTS";
	

	/**
	 * TODO: Indexes can only be created once. Commented out for now - new to be smarter about this
	 * Create all structure on initialization if it doesn't exist. Data that is already there will remain.
	 */
	public DatabaseService(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
		this.jdbcTemplate = jdbcTemplate;
		this.transactionManager = transactionManager;
		String createPatients = "CREATE TABLE IF NOT EXISTS " + TABLE_PATIENTS + " (\n" + 
				"   ROW_ID INT(11) NOT NULL,\n" + 
				"   SUBJECT_ID INT(11) NOT NULL,\n" + 
				"   GENDER VARCHAR(255) NOT NULL,	\n" + 
				"   DOB VARCHAR(255) NOT NULL,	\n" + 
				"   DOD DATETIME,\n" + 
				"   DOD_HOSP DATETIME,\n" + 
				"   DOD_SSN DATETIME,\n" + 
				"   EXPIRE_FLAG TINYINT UNSIGNED NOT NULL,\n" + 
				"  UNIQUE KEY PATIENTS_ROW_ID (ROW_ID),	\n" + 
				"  UNIQUE KEY PATIENTS_SUBJECT_ID (SUBJECT_ID)	\n" + 
				"  )";
		
		jdbcTemplate.execute(createPatients);
		
		String createAdmissions = "CREATE TABLE IF NOT EXISTS " + TABLE_ADMISSIONS + " (\n" + 
				"   ROW_ID INTEGER(11) NOT NULL,\n" + 
				"   SUBJECT_ID INTEGER(11) NOT NULL,\n" + 
				"   HADM_ID INTEGER(11) NOT NULL,\n" + 
				"   ADMITTIME DATETIME NOT NULL,\n" + 
				"   DISCHTIME DATETIME NOT NULL,\n" + 
				"   DEATHTIME DATETIME,\n" + 
				"   ADMISSION_TYPE VARCHAR(255) NOT NULL,\n" + 
				"   ADMISSION_LOCATION VARCHAR(255) NOT NULL,\n" + 
				"   DISCHARGE_LOCATION VARCHAR(255) NOT NULL,\n" + 
				"   INSURANCE VARCHAR(255) NOT NULL,\n" + 
				"   LANGUAGE VARCHAR(255),\n" + 
				"   RELIGION VARCHAR(255),\n" + 
				"   MARITAL_STATUS VARCHAR(255),\n" + 
				"   ETHNICITY VARCHAR(255) NOT NULL,\n" + 
				"   EDREGTIME DATETIME,\n" + 
				"   EDOUTTIME DATETIME,\n" + 
				"   DIAGNOSIS TEXT,	\n" + 
				"   HOSPITAL_EXPIRE_FLAG TINYINT UNSIGNED NOT NULL,\n" + 
				"   HAS_CHARTEVENTS_DATA TINYINT UNSIGNED NOT NULL,\n" + 
				"  UNIQUE KEY ADMISSIONS_ROW_ID (ROW_ID),\n" + 
				"  UNIQUE KEY ADMISSIONS_HADM_ID (HADM_ID)\n" + 
				"  )\n" + 
				"  CHARACTER SET = UTF8;";
		
		jdbcTemplate.execute(createAdmissions);
		
		String indexAdmissions = "alter table " + TABLE_ADMISSIONS + "\n" + 
				"      add index ADMISSIONS_IDX01 (SUBJECT_ID,HADM_ID),\n" + 
				"      add index ADMISSIONS_IDX02 (ADMITTIME, DISCHTIME, DEATHTIME);";

		//jdbcTemplate.execute(indexAdmissions);

		String createDiagnosesIcd = "CREATE TABLE IF NOT EXISTS " + TABLE_DIAGNOSES_ICD + " (\n" + 
				"   ROW_ID INTEGER NOT NULL,\n" + 
				"   SUBJECT_ID INTEGER NOT NULL,\n" + 
				"   HADM_ID INTEGER NOT NULL,\n" + 
				"   SEQ_NUM TINYINT UNSIGNED,\n" + 
				"   ICD9_CODE VARCHAR(255),	\n" + 
				"  UNIQUE KEY DIAGNOSES_ICD_ROW_ID (ROW_ID)	\n" + 
				"  )\n" + 
				"  CHARACTER SET = UTF8;\n";

		jdbcTemplate.execute(createDiagnosesIcd);
		
		String indexDiagnosesIcd = "alter table " + TABLE_DIAGNOSES_ICD + "\n" + 
				"      add index DIAGNOSES_ICD_idx01 (SUBJECT_ID, HADM_ID),\n" + 
				"      add index DIAGNOSES_ICD_idx02 (ICD9_CODE, SEQ_NUM);";

		//jdbcTemplate.execute(indexDiagnosesIcd);

		String createLabItem = "CREATE TABLE IF NOT EXISTS " + TABLE_D_LABITEMS + " (\n" + 
				"   ROW_ID INTEGER NOT NULL,\n" + 
				"   ITEMID INTEGER NOT NULL,\n" + 
				"   LABEL VARCHAR(255) NOT NULL,\n" + 
				"   FLUID VARCHAR(255) NOT NULL,\n" + 
				"   CATEGORY VARCHAR(255) NOT NULL,\n" + 
				"   LOINC_CODE VARCHAR(255),\n" + 
				"  UNIQUE KEY D_LABITEMS_ROW_ID (ROW_ID),\n" + 
				"  UNIQUE KEY D_LABITEMS_ITEMID (ITEMID)\n" + 
				"  )\n" + 
				"  CHARACTER SET = UTF8;\n";

		jdbcTemplate.execute(createLabItem);
		
		String indexLabItem = "alter table " + TABLE_D_LABITEMS + "\n" + 
				"      add index D_LABITEMS_idx03 (LOINC_CODE);\n";

		//jdbcTemplate.execute(indexLabItem);

		String createLabEvent = "CREATE TABLE IF NOT EXISTS " + TABLE_LABEVENTS + " (\n" + 
				"   ROW_ID INTEGER NOT NULL,\n" + 
				"   SUBJECT_ID INTEGER UNSIGNED NOT NULL,\n" + 
				"   HADM_ID INTEGER UNSIGNED,\n" + 
				"   ITEMID INTEGER UNSIGNED NOT NULL,\n" + 
				"   CHARTTIME DATETIME NOT NULL,\n" + 
				"   VALUE TEXT,	\n" + 
				"   VALUENUM FLOAT,\n" + 
				"   VALUEUOM VARCHAR(255),\n" + 
				"   FLAG VARCHAR(255),\n" + 
				"   REF_LOW VARCHAR(255),\n" + 
				"   REF_HIGH VARCHAR(255),\n" + 
				"  UNIQUE KEY LABEVENTS_ROW_ID (ROW_ID)	\n" + 
				"  )\n" + 
				"  CHARACTER SET = UTF8;\n";

		jdbcTemplate.execute(createLabEvent);
		
		String indexLabEvent = "alter table " + TABLE_LABEVENTS + "\n" + 
				"      add index LABEVENTS_idx01 (SUBJECT_ID, HADM_ID),\n" + 
				"      add index LABEVENTS_idx02 (ITEMID),\n" + 
				"      add index LABEVENTS_idx03 (CHARTTIME),\n" + 
				"      add index LABEVENTS_idx04 (VALUE(255), VALUENUM);\n";

		//jdbcTemplate.execute(indexLabEvent);

	}
	
	public void truncate(String tableName) {
		jdbcTemplate.execute("TRUNCATE " + tableName);
	}

	public void insertPatients(List<Patient> patients) {		
		String sql = "INSERT INTO " + TABLE_PATIENTS + " (ROW_ID, SUBJECT_ID, GENDER, DOB, EXPIRE_FLAG)\n" + 
				"VALUES (?, ?, ?, ?, ?)";

		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						Patient patient = patients.get(i);
						ps.setInt(1, patient.getRowId());
						ps.setInt(2, patient.getSubjectId());
						ps.setString(3, patient.getGender());
						ps.setString(4, patient.getDob());
						ps.setInt(5, patient.getExpireFlag());
					}

					@Override
					public int getBatchSize() {
						return patients.size();
					}
				});
			}
		});
	}

	public void insertAdmissions(List<Admission> admissions) {		
		String sql = "INSERT INTO " + TABLE_ADMISSIONS + " (ROW_ID, SUBJECT_ID, HADM_ID, ADMITTIME, DISCHTIME, "
				+ "ADMISSION_TYPE, ADMISSION_LOCATION, DISCHARGE_LOCATION, INSURANCE, ETHNICITY, HOSPITAL_EXPIRE_FLAG, HAS_CHARTEVENTS_DATA)\n" + 
				"VALUES (?, ?, ?, ?, ?, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 0, 0)";

		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						Admission admission = admissions.get(i);
						ps.setInt(1, admission.getRowId());
						ps.setInt(2, admission.getSubjectId());
						ps.setInt(3, admission.getHadmId());
						ps.setTimestamp(4, admission.getAdmitTime());
						ps.setTimestamp(5, admission.getDischTime());
					}

					@Override
					public int getBatchSize() {
						return admissions.size();
					}
				});
			}
		});
	}

	public void insertDiagnosesICD(List<DiagnosisICD> diagnoses) {		
		String sql = "INSERT INTO " + TABLE_DIAGNOSES_ICD + " (ROW_ID, SUBJECT_ID, HADM_ID, SEQ_NUM, ICD9_CODE)\n" + 
				"VALUES (?, ?, ?, 0, ?)";

		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						DiagnosisICD diagnosis = diagnoses.get(i);
						ps.setInt(1, diagnosis.getRowId());
						ps.setInt(2, diagnosis.getSubjectId());
						ps.setInt(3, diagnosis.getHadmId());
						ps.setString(4, diagnosis.getIcd9Code());
					}

					@Override
					public int getBatchSize() {
						return diagnoses.size();
					}
				});
			}
		});
	}

	public void insertLabItems(List<LabItem> labItems) {		
		String sql = "INSERT INTO " + TABLE_D_LABITEMS + " (ROW_ID, ITEMID, LABEL, FLUID, CATEGORY, LOINC_CODE)\n" + 
				"VALUES (?, ?, ?, 'N/A', 'N/A', ?)";

		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						LabItem labItem = labItems.get(i);
						ps.setInt(1, labItem.getRowId());
						ps.setInt(2, labItem.getItemId());
						ps.setString(3, labItem.getLabel());
						ps.setString(4, labItem.getLoincCode());
					}

					@Override
					public int getBatchSize() {
						return labItems.size();
					}
				});
			}
		});
	}

	public void insertLabEvents(List<LabEvent> labEvents) {		
		String sql = "INSERT INTO " + TABLE_LABEVENTS + " (ROW_ID, SUBJECT_ID, HADM_ID, ITEMID, CHARTTIME, VALUE, VALUENUM, VALUEUOM, FLAG, REF_LOW, REF_HIGH)\n" + 
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {

			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						LabEvent labEvent = labEvents.get(i);
						ps.setInt(1, labEvent.getRowId());
						ps.setInt(2, labEvent.getSubjectId());
						if (labEvent.getHadmId() != null) {
							ps.setInt(3, labEvent.getHadmId());
						} else {
							ps.setNull(3, Types.INTEGER);
						}
						ps.setInt(4, labEvent.getItemId());
						ps.setTimestamp(5, labEvent.getChartTime());
						ps.setString(6, labEvent.getValue());
						if (labEvent.getValueNum() != null) {
							ps.setDouble(7, labEvent.getValueNum());
						} else {
							ps.setNull(7, Types.DOUBLE);
						}
						ps.setString(8, labEvent.getValueUom());
						ps.setString(9, labEvent.getFlag());
						ps.setString(10, labEvent.getRefLow());
						ps.setString(11, labEvent.getRefHigh());
					}

					@Override
					public int getBatchSize() {
						return labEvents.size();
					}
				});
			}
		});
	}

}
