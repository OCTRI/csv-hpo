package org.octri.csvhpo.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvMalformedLineException;

@Service
public class LoadDataService {

	private static final Logger logger = LoggerFactory.getLogger(LoadDataService.class);

	@Autowired
	DatabaseService databaseService;

	int batchSize = 10000;

	public void loadAll(String csvDirectory, boolean convertToMimic) throws IOException {
		logger.info("Loading csv files into database.");
		long startTime = System.currentTimeMillis();
		//loadPatients(csvDirectory, convertToMimic);
		//loadAdmissions(csvDirectory, convertToMimic);
		//loadDiagnosesICD(csvDirectory, convertToMimic);
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
				diagnosis.setIcd10Code(line[3]);
			} else {
				diagnosis.setRowId(Integer.parseInt(line[0]));
				diagnosis.setSubjectId(Integer.parseInt(line[1]));
				diagnosis.setHadmId(Integer.parseInt(line[2]));
				diagnosis.setIcd9Code(line[4]);
			}

			// In the OHSU data set, look for the followup flag. Remove if Y
			if (line.length == 16 && line[15].equals("Y")) {
				// skip
			} else {
				diagnoses.add(diagnosis);
			}

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
		
		logger.info("Converting and loading labs");

		Map<String,Map<String,LabItem>> labItemMap = new HashMap<>();
		List<LabEvent> labEvents = new ArrayList<>(batchSize);

		CSVReader csvReader = getReader(path + "/labevents.csv");
		String[] line;
		int count = 0;
		int items = 0;
		boolean more = true;
		while (more) {
			try {
				line = csvReader.readNext();
				if (line == null) {
					more = false;
					continue;
				}
				count++;
				LabEvent labEvent = new LabEvent();
				labEvent.setRowId(count);
				labEvent.setSubjectId(Integer.parseInt(line[0]));
				labEvent.setHadmId(StringUtils.isEmpty(line[1])?null:Integer.parseInt(line[1]));
				
				// TODO: This is OHSU specific. The internal id for lab type sometimes has different LOINCs associated.
				// When the LOINC is null but one is available elsewhere, we want to use it.
				// Concatenate the internal id and name to be the key for this lab item
				String labItemType = line[2] + "|" + line[11];
				String loinc = StringUtils.defaultIfBlank(line[3], null);
				if (labItemMap.containsKey(labItemType)) {
					Map<String, LabItem> labItemsByLoinc = labItemMap.get(labItemType);
					// If this has a LOINC, see if we've got a match
					LabItem item = labItemsByLoinc.get(loinc);
					if (loinc == null && item == null) {
						// This lab has a null LOINC. Assign to a matching lab item with a LOINC
						item = labItemsByLoinc.values().iterator().next();
					} else if (loinc != null && item == null) {
						item = labItemsByLoinc.get(null);
						if (item != null) {
							// This lab has a LOINC and a matching lab item has a null LOINC. Reassign the null to this LOINC.
							labItemsByLoinc.remove(null);
							item.setLoincCode(loinc);
						} else {
							// This lab has a LOINC that is different from the lab items already entered. Create a new lab item.
							items++;
							item = new LabItem();
							item.setRowId(items);
							item.setItemId(items);
							item.setCategory(line[2]);
							item.setLabel(line[11]);
							item.setLoincCode(loinc);
						}
						labItemsByLoinc.put(item.getLoincCode(), item);
					}					
					labEvent.setItemId(item.getItemId());
					
				} else {
					// This is the first instance of this lab. Create a new lab item.
					items++;
					LabItem labItem = new LabItem();
					labItem.setRowId(items);
					labItem.setItemId(items);
					labItem.setCategory(line[2]);
					labItem.setLabel(line[11]);
					labItem.setLoincCode(loinc);
					Map<String, LabItem> loincMap = new HashMap<>();
					loincMap.put(loinc, labItem);
					labItemMap.put(labItemType, loincMap);
					labEvent.setItemId(items);
				}
				
				String flag;
				if (line[8] == null || line[8].equals("N")) {
					flag = "ABNORMAL";
				} else if (line[8].equals("Y")) {
					flag = "NORMAL";
				} else {
					flag = "UNKNOWN";
				}
			
				Optional<Timestamp> chartTime = getTimestampForDateString(line[4]);
				labEvent.setChartTime(chartTime.get());
				labEvent.setValue(line[6]);
				labEvent.setValueNum(StringUtils.isEmpty(line[7])?null:Double.parseDouble(line[7]));
				labEvent.setValueUom(line[5]);
				labEvent.setFlag(flag);
				labEvent.setRefLow(line[9]);
				labEvent.setRefHigh(line[10]);

				// TODO: Make this an option to override flags
				if (StringUtils.isNoneEmpty(labEvent.getRefLow(), labEvent.getRefHigh(), line[7]) && labEvent.getFlag().equals("UNKNOWN")) {
					// Set the flag based on the reference ranges
					try {
						Double low = Double.parseDouble(labEvent.getRefLow());
						Double high = Double.parseDouble(labEvent.getRefHigh());
						if (labEvent.getValueNum() < low || labEvent.getValueNum() > high) {
							labEvent.setFlag("ABNORMAL");
						} else {
							labEvent.setFlag("NORMAL");
						}
					} catch (NumberFormatException e) {
						// Oh well, we tried
					}
				}

				labEvents.add(labEvent);
	
				if (labEvents.size() == batchSize) {
					databaseService.insertLabEvents(labEvents);
					labEvents.clear();
				}
			
			} catch (CsvMalformedLineException e) {
				// We can't recover from this, and we get in an endless loop if it happens. The file will have to be corrected at the source.
				logger.error("Failed after " + csvReader.getRecordsRead() + " lines");
				e.printStackTrace();
				break;
			} catch (Exception e) {
				// This may happen if a stray comma or quote is encountered. Whole record is screwy, so just move on
				continue;
			}

		}

		// Write the last batch
		databaseService.insertLabEvents(labEvents);

		csvReader.close();
		logger.info("Loaded " + count + " lab events");
		
		// Do this in batches too
		List<LabItem> labItems = new ArrayList<>();
		for (Map<String, LabItem> labItem : labItemMap.values()) {
			for (LabItem i : labItem.values()) {
				labItems.add(i);
				
				if (labItems.size() == batchSize) {
					databaseService.insertLabItems(labItems);
					labItems.clear();
				}
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
		final CSVParser parser =
				new CSVParserBuilder()
				.withSeparator('\t')
				.withIgnoreQuotations(true)
				.build();
		final CSVReader csvReader =
				new CSVReaderBuilder(new BufferedReader(new FileReader(path)))
				.withSkipLines(1)
				.withCSVParser(parser)
				.build();
		return csvReader;
	}

	private Optional<Timestamp> getTimestampForDateString(String dateString) {
		if (dateString == null) {
			return Optional.empty();
		}

		List<SimpleDateFormat> dateFormats = new ArrayList<>();
		dateFormats.add(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
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
