package org.octri.csvhpo.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.octri.csvhpo.domain.LabSummary;
import org.octri.csvhpo.domain.LabSummaryStatistics;
import org.octri.csvhpo.util.ResultSetUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class LabSummaryService {

    @Autowired
    JdbcTemplate jdbcTemplate;

    //TODO: This query only works for the MIMIC data set where the flag is known. Can we just assume all normal for stats purposes?
	private final String labSummaryStatistics = "CREATE table IF NOT EXISTS lab_summary_statistics as (select a.itemid, valueuom, i.loinc_code as 'loinc', counts, mean_all, min_normal, mean_normal, max_normal from\n" + 
			"(select e.itemid, e.valueuom, count(*) as counts, avg(valuenum) as mean_all,\n" + 
			"min(case when (valuenum IS NOT NULL and (flag IS NULL OR UPPER(flag)!='ABNORMAL')) then valuenum else null end) as min_normal,\n" + 
			"avg(case when (valuenum IS NOT NULL and (flag IS NULL OR UPPER(flag)!='ABNORMAL')) then valuenum else null end) as mean_normal,\n" + 
			"max(case when (valuenum IS NOT NULL and (flag IS NULL OR UPPER(flag)!='ABNORMAL')) then valuenum else null end) as max_normal\n" + 
			"from labevents e\n" + 
			"GROUP BY e.itemid, e.valueuom) as a\n" + 
			"join d_labitems i on a.itemid = i.itemid)";

	public void createLabSummaryStatistics() {
		jdbcTemplate.execute("DROP table IF EXISTS lab_summary_statistics;");
		jdbcTemplate.execute(labSummaryStatistics);
	}
	
	public List<LabSummaryStatistics> getLabSummaryStatistics() {
		return jdbcTemplate.query("select * from lab_summary_statistics", getRowMapper());
	}
	
	public Map<Integer, LabSummary> getLabSummaryMap() {
		
	    Map<Integer, LabSummary> labSummaryMap = new HashMap<>();
		
	    List<LabSummaryStatistics> stats = getLabSummaryStatistics();
        for (LabSummaryStatistics stat : stats) {
            int itemId = stat.getItemId();
            String unit = stat.getValueuom();
            String loinc = stat.getLoinc();
            int count = stat.getCounts();
            Double mean = stat.getMean_all();
            Double min_normal = stat.getMin_normal();
            Double max_normal = stat.getMax_normal();
            labSummaryMap.putIfAbsent(stat.getItemId(), new LabSummary(itemId, loinc));
            labSummaryMap.get(itemId).put(unit, count, mean, min_normal, max_normal);
        }

        return labSummaryMap;
	}
	
	private RowMapper<LabSummaryStatistics> getRowMapper() {
	
		return new RowMapper<LabSummaryStatistics>(){  
	    
			@Override  
			public LabSummaryStatistics mapRow(ResultSet rs, int rownumber) throws SQLException {  
				LabSummaryStatistics statistics = new LabSummaryStatistics();
				statistics.setItemId(rs.getInt("itemid"));
				statistics.setValueuom(rs.getString("valueuom"));
				statistics.setLoinc(rs.getString("loinc"));
				statistics.setCounts(rs.getInt("counts"));
				statistics.setMean_all(ResultSetUtil.getDoubleOrNull(rs, "mean_all"));
				statistics.setMin_normal(ResultSetUtil.getDoubleOrNull(rs, "min_normal"));
				statistics.setMean_normal(ResultSetUtil.getDoubleOrNull(rs, "mean_normal"));
				statistics.setMax_normal(ResultSetUtil.getDoubleOrNull(rs, "max_normal"));
				return statistics;
			}  
		};
		
	}

}
