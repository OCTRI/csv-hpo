package org.octri.csvhpo;

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
	LoadDataService loadDataService;

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
        }
        
        if (convert) {
        	// The labs need to be converted
        	logger.info("Converting labs to HPO");
        }
        
	}

}
