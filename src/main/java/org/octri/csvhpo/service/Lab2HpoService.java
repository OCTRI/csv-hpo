package org.octri.csvhpo.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.monarchinitiative.loinc2hpo.exception.LoincCodeNotAnnotatedException;
import org.monarchinitiative.loinc2hpo.exception.LoincCodeNotFoundException;
import org.monarchinitiative.loinc2hpo.exception.MalformedLoincCodeException;
import org.monarchinitiative.loinc2hpo.exception.UnrecognizedCodeException;
import org.monarchinitiative.loinc2hpo.loinc.HpoTerm4TestOutcome;
import org.octri.csvhpo.domain.LabEvent;
import org.octri.csvhpo.domain.LabHpo;
import org.octri.csvhpo.exception.UnableToInterpretException;
import org.octri.csvhpo.lab2hpo.LabEvents2HpoFactory;
import org.octri.csvhpo.util.ResultSetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class Lab2HpoService {

    private static final Logger logger = LoggerFactory.getLogger(Lab2HpoService.class);

    @Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	private PlatformTransactionManager transactionManager;

	private final String maxRowIdQuery = "select max(row_id) from labevents";
	private final String allLabsQuery = "select * from labevents";

	public void labToHpo(LabEvents2HpoFactory labConvertFactory) {
		// The fetch size will limit how many results come back at once reducing memory
		// requirements. This is the specific recommendation for MySQL and may need to
		// be varied for other dbs
		jdbcTemplate.setFetchSize(Integer.MIN_VALUE);
		initTable();
		try {
			initErrorTable();
		} catch (SQLException e) {
			e.printStackTrace();
			//TODO: warn user
			return;
		}
		Integer maxRowId = jdbcTemplate.queryForObject(maxRowIdQuery, null, Integer.class);
		logger.info("Computing HPO terms up to row " + maxRowId);
		jdbcTemplate.query(allLabsQuery,
				new LabEventCallbackHandler(jdbcTemplate, transactionManager, labConvertFactory, maxRowId));
	}

	public void initTable() {
		String query = "CREATE TABLE IF NOT EXISTS LABHPO (ROW_ID INT UNSIGNED NOT NULL, NEGATED VARCHAR(5), MAP_TO VARCHAR(255), PRIMARY KEY (ROW_ID));";
		jdbcTemplate.execute(query);
		jdbcTemplate.execute("TRUNCATE LABHPO;");
	}

	public void initErrorTable() throws SQLException {
		jdbcTemplate.execute("DROP TABLE IF EXISTS D_LAB2HPO_MAP_ERR;");
		jdbcTemplate.execute("CREATE TABLE D_LAB2HPO_MAP_ERR (\n" + 
				"   ERR_ID VARCHAR(7) NOT NULL, \n" + 
				"   ERR_LABEL VARCHAR(255) NOT NULL, \n" + 
				"   PRIMARY KEY (ERR_ID)\n" + 
				"  );");
		jdbcTemplate.execute("INSERT IGNORE INTO D_LAB2HPO_MAP_ERR(ERR_ID, ERR_LABEL)\n" + 
				"VALUES \n" + 
				"    ('ERROR1', 'local id not mapped to loinc'),\n" + 
				"    ('ERROR2', 'malformed loinc id'),\n" + 
				"    ('ERROR3', 'loinc code not annotated'),\n" + 
				"    ('ERROR4', 'interpretation code not mapped to hpo'),\n" + 
				"    ('ERROR5', 'unable to interpret'),\n" + 
				"    ('ERROR6', 'unrecognized unit');");
	}

	private class LabEventCallbackHandler implements RowCallbackHandler {

		int batchSize = 20000;
		JdbcTemplate jdbcTemplate;
		TransactionTemplate transactionTemplate;
		LabEvents2HpoFactory labConvertFactory;
		Integer maxRowId;
		List<LabHpo> labHpos = new ArrayList<>(batchSize);

		LabEventCallbackHandler(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager,
				LabEvents2HpoFactory labConvertFactory, Integer maxRowId) {
			this.jdbcTemplate = jdbcTemplate;
			this.transactionTemplate = new TransactionTemplate(transactionManager);
			this.labConvertFactory = labConvertFactory;
			this.maxRowId = maxRowId;
		}

		@Override
		public void processRow(ResultSet rs) {
			try {
				LabEvent labEvent = parseLabEvent(rs);
				int rowId = labEvent.getRowId();
				LabHpo labHpo = null;
	
				Optional<HpoTerm4TestOutcome> outcome = null;
				try {
					outcome = labConvertFactory.convert(labEvent);
					boolean negated = false;
					String mappedHpo = "?";
					if (outcome.isPresent()) {
						negated = outcome.get().isNegated();
						mappedHpo = outcome.get().getId().getValue();
					}
					labHpo = new LabHpo(rowId, negated ? "T" : "F", mappedHpo);
				} catch (LoincCodeNotFoundException e) {
					//ERROR 1: local id not mapped to loinc
					//Look up the D_LAB2HPO_MAP_ERR table for error code
					labHpo = new LabHpo(rowId, "U", "ERROR1");
				} catch (MalformedLoincCodeException e) {
					//ERROR 2: malformed loinc id
					labHpo = new LabHpo(rowId, "U", "ERROR2");
				} catch (LoincCodeNotAnnotatedException e) {
					//ERROR 3: loinc code not annotated
					labHpo = new LabHpo(rowId, "U", "ERROR3");
				} catch (UnrecognizedCodeException e) {
					//ERROR 4: interpretation code not mapped to hpo
					labHpo = new LabHpo(rowId, "U", "ERROR4");
				} catch (UnableToInterpretException e) {
					//ERROR 5: unable to interpret
					labHpo = new LabHpo(rowId, "U", "ERROR5");
				} 
	
				labHpos.add(labHpo);
				
				if (labHpos.size() == batchSize || maxRowId.equals(rowId)) {
					logger.info("Inserting batch of LabHpo records of size: " + labHpos.size());
					insertBatch(labHpos);
					labHpos.clear();
				}
			} catch (Exception e) {
				// TODO: This could be better handled, but gives visibility into what's wrong in the conversion
				e.printStackTrace();
			}

		}

		public void insertBatch(final List<LabHpo> labHpos) {

			String sql = "INSERT INTO LABHPO " + "(ROW_ID, NEGATED, MAP_TO) VALUES (?, ?, ?)";

			transactionTemplate.execute(new TransactionCallbackWithoutResult() {

				@Override
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

						@Override
						public void setValues(PreparedStatement ps, int i) throws SQLException {
							LabHpo labHpo = labHpos.get(i);
							ps.setInt(1, labHpo.getRowid());
							ps.setString(2, labHpo.getNegated());
							ps.setString(3, labHpo.getMapTo());
						}

						@Override
						public int getBatchSize() {
							return labHpos.size();
						}
					});
				}
			});
		}
		
		private LabEvent parseLabEvent(ResultSet rs) throws SQLException {
			int row_id = rs.getInt("ROW_ID");
			int subject_id = rs.getInt("SUBJECT_ID");
			int hadm_id = rs.getInt("HADM_ID");
			int item_id = rs.getInt("ITEMID");
			Timestamp charttime = rs.getTimestamp("CHARTTIME");
			String value = ResultSetUtil.getStringOrBlank(rs, "VALUE").trim().replace("\"", "");
	        Double valuenum;
	        if (StringUtils.isBlank(rs.getString("VALUENUM"))) {
	        	valuenum = null;
	        } else {
	            valuenum = Double.parseDouble(rs.getString("VALUENUM").trim());
	        }
	        String valueuom = ResultSetUtil.getStringOrBlank(rs, "VALUEUOM").trim().replace("\"", "");
	        String flag = ResultSetUtil.getStringOrBlank(rs, "FLAG").trim().replace("\"", "");
	        String refLow = ResultSetUtil.getStringOrBlank(rs, "REF_LOW").trim().replace("\"", "");
	        String refHigh = ResultSetUtil.getStringOrBlank(rs, "REF_HIGH").trim().replace("\"", "");

			LabEvent labEvent = new LabEvent(row_id, subject_id, hadm_id, item_id, charttime, value,
					valuenum, valueuom, flag, refLow, refHigh);
			
			return labEvent;		
		}


	}

}
