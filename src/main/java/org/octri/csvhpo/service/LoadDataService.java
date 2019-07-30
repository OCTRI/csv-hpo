package org.octri.csvhpo.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.octri.csvhpo.domain.Admission;
import org.octri.csvhpo.domain.DiagnosisICD;
import org.octri.csvhpo.domain.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.opencsv.CSVReader;

@Service
public class LoadDataService {

	private static final Logger logger = LoggerFactory.getLogger(LoadDataService.class);

	@Autowired
	DatabaseService databaseService;

	int batchSize = 10000;

	public void loadAll(String csvDirectory, boolean convertToMimic) throws IOException {
		logger.info("Loading csv files into database.");
		long startTime = System.currentTimeMillis();
		// loadPatients(csvDirectory + (csvDirectory.endsWith("/") ? "patients.csv":"/patients.csv"), convertToMimic);
		// loadAdmissions(csvDirectory + (csvDirectory.endsWith("/") ? "admissions.csv" : "/admissions.csv"), convertToMimic);
		loadDiagnosesICD(csvDirectory + (csvDirectory.endsWith("/") ? "diagnoses_icd.csv":"/diagnoses_icd.csv"), convertToMimic);
		long endTime = System.currentTimeMillis();
		logger.info("Data load complete in " + (endTime - startTime)*(1.0)/1000 + " seconds.");

	}

	public void loadPatients(String path, boolean convertToMimic) throws IOException {
		
		logger.info("Loading patients");

		List<Patient> patients = new ArrayList<>(batchSize);
		databaseService.truncate(DatabaseService.TABLE_PATIENTS);
		CSVReader csvReader = getReader(path);
		String[] line;
		int count = 0;
		while ((line = csvReader.readNext()) != null) {
			ArrayList<String> fields = new ArrayList<>(Arrays.asList(line));
			// Insert a fake column, so things line up
			if (convertToMimic) {
				fields.add(0, null);
			}
			Patient patient = new Patient();
			Integer id = Integer.parseInt(fields.get(1));
			patient.setRowId(id);
			patient.setSubjectId(id);
			patient.setGender(fields.get(2));
			patient.setDob(fields.get(3));

			patients.add(patient);

			if (patients.size() == batchSize) {
				databaseService.insertPatients(patients);
				patients.clear();
			}

			count++;

		}

		// Write the last of the patients
		databaseService.insertPatients(patients);

		csvReader.close();
		logger.info("Loaded " + count + " patients");
	}

	public void loadAdmissions(String path, boolean convertToMimic) throws IOException {

		logger.info("Loading admissions");
		
		List<Admission> admissions = new ArrayList<>(batchSize);
		databaseService.truncate(DatabaseService.TABLE_ADMISSIONS);
		CSVReader csvReader = getReader(path);
		String[] line;
		int count = 0;
		while ((line = csvReader.readNext()) != null) {
			Admission admission = new Admission();
			if (convertToMimic) {
				admission.setSubjectId(Integer.parseInt(line[0]));
				admission.setRowId(Integer.parseInt(line[1]));
				admission.setHadmId(Integer.parseInt(line[1]));
				Optional<Timestamp> admitTime = getTimestampForDateString(line[2]);
				if (!admitTime.isPresent()) {
					throw new RuntimeException("Could not parse timestamp " + line[2]);
				}
				admission.setAdmitTime(admitTime.get());
				// Some systems may not have a discharge time. Set to admit time.
				Optional<Timestamp> dischTime = getTimestampForDateString(line[3]);
				admission.setDischTime(dischTime.isPresent() ? dischTime.get() : admitTime.get());
			} else {
				admission.setRowId(Integer.parseInt(line[0]));
				admission.setSubjectId(Integer.parseInt(line[1]));
				admission.setHadmId(Integer.parseInt(line[2]));
				admission.setAdmitTime(Timestamp.valueOf(line[3]));
				admission.setDischTime(Timestamp.valueOf(line[4]));
			}

			admissions.add(admission);

			if (admissions.size() == batchSize) {
				databaseService.insertAdmissions(admissions);
				admissions.clear();
			}

			count++;

		}

		// Write the last batch
		databaseService.insertAdmissions(admissions);

		csvReader.close();
		logger.info("Loaded " + count + " admissions");
	}

	public void loadDiagnosesICD(String path, boolean convertToMimic) throws IOException {
		
		// TODO: OHSU data has more ICD10 codes than ICD9. Add structure for this.
		// TODO: There are several flags that may be useful. For example, some diagnoses are assigned with a followup flag, meaning they are not really final.

		logger.info("Loading diagnoses");

		List<DiagnosisICD> diagnoses = new ArrayList<>(batchSize);
		databaseService.truncate(DatabaseService.TABLE_DIAGNOSES_ICD);
		CSVReader csvReader = getReader(path);
		String[] line;
		int count = 0;
		while ((line = csvReader.readNext()) != null) {
			count++;
			DiagnosisICD diagnosis = new DiagnosisICD();
			if (convertToMimic) {
				diagnosis.setSubjectId(Integer.parseInt(line[0]));
				diagnosis.setRowId(count);
				diagnosis.setHadmId(Integer.parseInt(line[1]));
				diagnosis.setIcd9Code(line[2]);
			} else {
				diagnosis.setRowId(Integer.parseInt(line[0]));
				diagnosis.setSubjectId(Integer.parseInt(line[1]));
				diagnosis.setHadmId(Integer.parseInt(line[2]));
				diagnosis.setIcd9Code(line[4]);
			}

			diagnoses.add(diagnosis);

			if (diagnoses.size() == batchSize) {
				databaseService.insertDiagnosesICD(diagnoses);
				diagnoses.clear();
			}

		}

		// Write the last batch
		databaseService.insertDiagnosesICD(diagnoses);

		csvReader.close();
		logger.info("Loaded " + count + " diagnoses");
	}

	/**
	 * Get the CSVReader and skip the first line
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private static CSVReader getReader(String path) throws IOException {
		Reader reader = new BufferedReader(new FileReader(path));
		CSVReader csvReader = new CSVReader(reader);
		csvReader.skip(1);
		return csvReader;
	}

	private Optional<Timestamp> getTimestampForDateString(String dateString) {
		if (dateString == null) {
			return Optional.empty();
		}

		List<SimpleDateFormat> dateFormats = new ArrayList<>();
		dateFormats.add(new SimpleDateFormat("M/d/yyyy"));

		for (SimpleDateFormat format : dateFormats) {
			try {
				Date date = format.parse(dateString);
				return Optional.of(Timestamp.from(date.toInstant()));
			} catch (ParseException e) {
				// Try the next format
			}
		}

		return Optional.empty();

	}

}
