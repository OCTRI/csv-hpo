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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.octri.csvhpo.domain.Admission;
import org.octri.csvhpo.domain.DiagnosisICD;
import org.octri.csvhpo.domain.LabEvent;
import org.octri.csvhpo.domain.LabItem;
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
		// loadPatients(csvDirectory, convertToMimic);
		// loadAdmissions(csvDirectory, convertToMimic);
		// loadDiagnosesICD(csvDirectory, convertToMimic);
		loadLabEvents(csvDirectory, convertToMimic);
		long endTime = System.currentTimeMillis();
		logger.info("Data load complete in " + (endTime - startTime)*(1.0)/1000 + " seconds.");

	}

	public void loadPatients(String path, boolean convertToMimic) throws IOException {
		
		logger.info("Loading patients");

		List<Patient> patients = new ArrayList<>(batchSize);
		databaseService.truncate(DatabaseService.TABLE_PATIENTS);
		CSVReader csvReader = getReader(path + "/patients.csv");
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
		CSVReader csvReader = getReader(path + "/admissions.csv");
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
		CSVReader csvReader = getReader(path + "/diagnoses_icd.csv");
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

	public void loadLabEvents(String path, boolean convertToMimic) throws IOException {
		
		databaseService.truncate(DatabaseService.TABLE_D_LABITEMS);
		databaseService.truncate(DatabaseService.TABLE_LABEVENTS);
		if (convertToMimic) {
			loadConvertedLabs(path);
		} else {
			loadMimicLabs(path);
		}
		
	}
	
	/**
	 * For conversion, keep track of the lab items by name/unit so we can build the foreign keys
	 * @param path
	 * @throws IOException 
	 */
	private void loadConvertedLabs(String path) throws IOException {
		
		Map<String,LabItem> labItemMap = new HashMap<>();
		List<LabEvent> labEvents = new ArrayList<>(batchSize);

		CSVReader csvReader = getReader(path + "/labevents.csv");
		String[] line;
		int count = 0;
		int items = 0;
		while ((line = csvReader.readNext()) != null) {
			count++;
			LabEvent labEvent = new LabEvent();
			labEvent.setRowId(count);
			labEvent.setSubjectId(Integer.parseInt(line[0]));
			labEvent.setHadmId(StringUtils.isEmpty(line[1])?null:Integer.parseInt(line[1]));
			
			boolean loincColumnFound = false;
			int i = 3; // First possible column it could be in 
			while (!loincColumnFound && i < line.length) {
				// The lab item may have commas in it, so we have to deal with that for now
				if (StringUtils.isEmpty(line[i]) || line[i].matches("^(\\d)+-(\\d)+$")) {
					loincColumnFound = true;
				}
				i++;
			}
			i--; // Set it back to the last one
			String itemName = line[2];
			for (int item=3; item < i; item++) {
				itemName += "," + line[item];
			}
			
			String loinc = line[i];
			String unit = line[i+2];
			
			String key = itemName + "|" + unit;
			LabItem labItem = labItemMap.get(key);
			if (labItem != null) {
				labEvent.setItemId(labItem.getItemId());
			} else {
				++items;
				labEvent.setItemId(items);
				labItem = new LabItem();
				labItem.setRowId(items);
				labItem.setItemId(items);
				labItem.setLabel(itemName);
				labItem.setLoincCode(loinc);
				labItemMap.put(key, labItem);
			}
			
			try {
				Optional<Timestamp> chartTime = getTimestampForDateString(line[i+1]);
				labEvent.setChartTime(chartTime.get());
				labEvent.setValue(line[i+3]);
				labEvent.setValueNum(StringUtils.isEmpty(line[i+4])?null:Double.parseDouble(line[i+4]));
				labEvent.setValueUom(unit);
				labEvent.setFlag(line[i+5]);
				labEvent.setRefLow(line[i+6]);
				labEvent.setRefHigh(line[i+7]);
			} catch (Exception e) {
				// This happens if the value has a comma in it or lab name ends with comma. Whole record is screwy, so just move on
				continue;
			}

			labEvents.add(labEvent);

			if (labEvents.size() == batchSize) {
				databaseService.insertLabEvents(labEvents);
				labEvents.clear();
			}

		}

		// Write the last batch
		databaseService.insertLabEvents(labEvents);

		csvReader.close();
		logger.info("Loaded " + count + " lab events");
		
		// Do this in batches too
		List<LabItem> labItems = new ArrayList<>();
		for (LabItem labItem : labItemMap.values()) {
			labItems.add(labItem);
			
			if (labItems.size() == batchSize) {
				databaseService.insertLabItems(labItems);
				labItems.clear();
			}
		}
		
		// Write the last batch
		databaseService.insertLabItems(labItems);
	}

	private void loadMimicLabs(String path) throws IOException {
		loadMimicLabItems(path);
		loadMimicLabEvents(path);
	}

	/**
	 * If this is the MIMIC format, there is a file for lab items. Read and insert as usual.
	 * @param path
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	private void loadMimicLabItems(String path) throws IOException {
		
		logger.info("Loading lab items");
		
		List<LabItem> labItems = new ArrayList<>(batchSize);
		
		CSVReader csvReader = getReader(path + "/d_labitems.csv");
		String[] line;
		int count = 0;
		while ((line = csvReader.readNext()) != null) {
			count++;
			LabItem labItem = new LabItem();
			labItem.setRowId(Integer.parseInt(line[0]));
			labItem.setItemId(Integer.parseInt(line[1]));
			labItem.setLabel(line[2]);
			labItem.setLoincCode(line[5]);

			labItems.add(labItem);

			if (labItems.size() == batchSize) {
				databaseService.insertLabItems(labItems);
				labItems.clear();
			}

		}

		// Write the last batch
		databaseService.insertLabItems(labItems);

		csvReader.close();
		logger.info("Loaded " + count + " lab items");
		
	}

	/**
	 * If this is the MIMIC format, lab items are already linked. Read and insert as usual.
	 * @param path
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	private void loadMimicLabEvents(String path) throws IOException {
		
		logger.info("Loading lab events");
		
		List<LabEvent> labEvents = new ArrayList<>(batchSize);
		
		CSVReader csvReader = getReader(path + "/labevents.csv");
		String[] line;
		int count = 0;
		while ((line = csvReader.readNext()) != null) {
			count++;
			LabEvent labEvent = new LabEvent();
			labEvent.setRowId(Integer.parseInt(line[0]));
			labEvent.setSubjectId(Integer.parseInt(line[1]));
			labEvent.setHadmId(StringUtils.isEmpty(line[2])?null:Integer.parseInt(line[2]));
			labEvent.setItemId(Integer.parseInt(line[3]));
			labEvent.setChartTime(Timestamp.valueOf(line[4]));
			labEvent.setValue(line[5]);
			labEvent.setValueNum(StringUtils.isEmpty(line[6])?null:Double.parseDouble(line[6]));
			labEvent.setValueUom(line[7]);
			labEvent.setFlag(line[8]);

			labEvents.add(labEvent);

			if (labEvents.size() == batchSize) {
				databaseService.insertLabEvents(labEvents);
				labEvents.clear();
			}

		}

		// Write the last batch
		databaseService.insertLabEvents(labEvents);

		csvReader.close();
		logger.info("Loaded " + count + " lab events");
		
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
		dateFormats.add(new SimpleDateFormat("M/d/yyyy h:m:s a"));

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
