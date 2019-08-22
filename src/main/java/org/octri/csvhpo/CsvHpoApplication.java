package org.octri.csvhpo;

import java.util.Map;

import org.monarchinitiative.loinc2hpo.io.LoincAnnotationSerializationFactory;
import org.monarchinitiative.loinc2hpo.loinc.LOINC2HpoAnnotationImpl;
import org.monarchinitiative.loinc2hpo.loinc.LoincEntry;
import org.monarchinitiative.loinc2hpo.loinc.LoincId;
import org.octri.csvhpo.domain.LabSummary;
import org.octri.csvhpo.lab2hpo.LabEvents2HpoFactory;
import org.octri.csvhpo.service.Lab2HpoService;
import org.octri.csvhpo.service.LabSummaryService;
import org.octri.csvhpo.service.LoadDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * This command line application runs the pipeline for converting labs to HPO terms. To run the full pipeline
 * pass the -a option and provide a directory where the followings csv files exist:
 * 
 * - patients.csv
 * - encounters.csv
 * - labs.csv
 * - diagnoses.csv
 * 
 * This can also be run one step at a time.
 * 
 * 1. --load-data <directory> or --load-mimic-data will load the csv files into the database. load-data transforms them to i2b2 format
 * 2. --summarize will take the loaded data and calculate stats for normal ranges on the labs
 * 3. --convert will take the labs plus summarized stats and convert each to HPO terms if possible
 * 
 * 
 * @author yateam
 *
 */
@SpringBootApplication
public class CsvHpoApplication implements CommandLineRunner {
	
    private static final Logger logger = LoggerFactory.getLogger(CsvHpoApplication.class);
	
	@Parameter(names={"--load-data", "-l"}, required = false)
	String csvDirectory;

	@Parameter(names={"--load-mimic-data", "-m"}, required = false)
	String mimicCsvDirectory;
	
	@Parameter(names= {"--summarize","-s"})
	boolean summarizeLabs;

	@Parameter(names= {"--convert", "-c"})
	boolean convert;
	
	@Autowired
	private LoadDataService loadDataService;

    @Autowired
    private LabSummaryService labSummaryService;
    
    @Autowired
    private Lab2HpoService lab2HpoService;

	public static void main(String[] args) {
		SpringApplication.run(CsvHpoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
        JCommander.newBuilder()
                .addObject(this)
                .build()
                .parse(args);
        
        if (csvDirectory != null) {
        	// The data needs to be loaded into the database
        	logger.info("Parsing flat files from the directory " + csvDirectory);
        	logger.info("These files will be converted to the MIMIC format and inserted into the database.");
        	loadDataService.loadAll(csvDirectory, true);
        }
 
        if (mimicCsvDirectory != null) {
        	// The data needs to be loaded into the database
        	logger.info("Parsing flat files from the directory " + mimicCsvDirectory);
        	logger.info("These files will be inserted into the database.");
        	loadDataService.loadAll(mimicCsvDirectory, false);
        }

        if (summarizeLabs) {
        	// The labs needs to be summarized
        	logger.info("Examining labs to determine the mean normal value for each type.");
        	labSummaryService.createLabSummaryStatistics();
        }
        
        if (convert) {
        	// The labs need to be converted
        	logger.info("Converting labs to HPO");
    		Map<Integer, LabSummary> labSummaryMap = labSummaryService.getLabSummaryMap();
    		
            Map<LoincId, LOINC2HpoAnnotationImpl> annotationMap = null;
            try {
                annotationMap = LoincAnnotationSerializationFactory.parseFromFile("src/main/resources/annotations.tsv", null, LoincAnnotationSerializationFactory.SerializationFormat.TSVSingleFile);
            } catch (Exception e) {
                logger.error("loinc2hpoAnnotation failed to load");
                e.printStackTrace();
                System.exit(1);
            }
            logger.info("loinc2hpoAnnotation successfully loaded.");
            logger.info("Total annotations: " + annotationMap.size());

            //load loincCoreTable
            Map<LoincId, LoincEntry> loincEntryMap = LoincEntry.getLoincEntryList("src/main/resources/LoincTableCore.csv");
            if (loincEntryMap.isEmpty()) {
                logger.error("loinc core table failed to load");
                System.exit(1);
            } else {
                logger.info("loinc core table successfully loaded");
                logger.info("loinc entries: " + loincEntryMap.size());
            }

            //start processing
            LabEvents2HpoFactory labConvertFactory = new LabEvents2HpoFactory(
                    labSummaryMap,
                    annotationMap,
                    loincEntryMap
            );

            lab2HpoService.labToHpo(labConvertFactory);


        }
        
	}

}
