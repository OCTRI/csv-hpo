-- These temporary tables summarize the data in a way that can be used by the mutual_info algorithm

CREATE TABLE IF NOT EXISTS p AS
(SELECT
		LABEVENTS.SUBJECT_ID, LABEVENTS.HADM_ID, LabHpo.MAP_TO
   FROM 
   		LABEVENTS 
   JOIN LabHpo on LABEVENTS.ROW_ID = LabHpo.ROW_ID
   	WHERE LabHpo.NEGATED = 'F' group by SUBJECT_ID, HADM_ID, MAP_TO
HAVING COUNT(*) > 1);