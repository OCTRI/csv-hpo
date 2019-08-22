package org.octri.csvhpo.lab2hpo;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.dstu3.model.Coding;
import org.monarchinitiative.loinc2hpo.codesystems.Code;
import org.monarchinitiative.loinc2hpo.exception.LoincCodeNotAnnotatedException;
import org.monarchinitiative.loinc2hpo.exception.LoincCodeNotFoundException;
import org.monarchinitiative.loinc2hpo.exception.MalformedLoincCodeException;
import org.monarchinitiative.loinc2hpo.exception.UnrecognizedCodeException;
import org.monarchinitiative.loinc2hpo.loinc.HpoTerm4TestOutcome;
import org.monarchinitiative.loinc2hpo.loinc.LOINC2HpoAnnotationImpl;
import org.monarchinitiative.loinc2hpo.loinc.LoincEntry;
import org.monarchinitiative.loinc2hpo.loinc.LoincId;
import org.octri.csvhpo.domain.LabEvent;
import org.octri.csvhpo.domain.LabSummary;
import org.octri.csvhpo.exception.UnableToInterpretException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Class that does the heavy lifting in this project: convert a labevent into HPO.
 */
public class LabEvents2HpoFactory {

    private static final Logger logger = LoggerFactory.getLogger(LabEvents2HpoFactory.class);

    // normal range for quantitative lab tests, key is the local lab test code
    private Map<Integer, LabSummary> labSummaryMap;
    // Loinc2Hpo annotations
    private Map<LoincId, LOINC2HpoAnnotationImpl> annotationMap;
    // Loinc entry
    private Map<LoincId, LoincEntry> loincEntryMap;


    private Set<Integer> failedQnWithTextResult = new HashSet<>();
    private Set<LoincId> qnNoLow = new HashSet<>();
    private Set<LoincId> qnNoHigh = new HashSet<>();

    public LabEvents2HpoFactory(Map<Integer, LabSummary> labSummaryMap,
                                Map<LoincId, LOINC2HpoAnnotationImpl> annotationMap,
                                Map<LoincId, LoincEntry> loincEntryMap) {
        this.labSummaryMap = labSummaryMap;
        this.annotationMap = annotationMap;
        this.loincEntryMap = loincEntryMap;

        Code low = new Code(new Coding().setCode("L").setSystem("FHIR"));
        Code high = new Code(new Coding().setCode("H").setSystem("FHIR"));
        for (LOINC2HpoAnnotationImpl annot : this.annotationMap.values()) {
            LoincId loincId = annot.getLoincId();
            LoincEntry loincEntry = loincEntryMap.get(loincId);
            if (loincEntry == null) {
                logger.error("loinc table does not have :" + loincId);
                System.exit(1);
            }
            String scale = loincEntry.getScale();
            if (scale.equals("Qn")) {
                if (!annot.getCandidateHpoTerms().containsKey(low)) {
                    qnNoLow.add(loincId);
                }
                if (!annot.getCandidateHpoTerms().containsKey(high)) {
                    qnNoHigh.add(loincId);
                }
            }
        }
    }

    public Optional<HpoTerm4TestOutcome> convert(LabEvent labEvent) throws LoincCodeNotFoundException, MalformedLoincCodeException, LoincCodeNotAnnotatedException, UnrecognizedCodeException, UnableToInterpretException {


//        logger.info("annotation map contains " + loincId.toString());
//        logger.info("annotation map for " + loincId.toString() + "candidate mappings: " + annotationMap.get(loincId).getCandidateHpoTerms().size());
    	int item_id = labEvent.getItemId();
        LabSummary labSummary = labSummaryMap.get(item_id);
        
        String loinc = labSummary.getLoinc();
        
        if (StringUtils.isBlank(loinc)) {
            //logger.warn("cannot map local item_id to loinc: " + item_id);
            throw new LoincCodeNotFoundException();
        }

        //throws MalformedLoincCodeException
        LoincId loincId = new LoincId(loinc);

        //check whether the loinc is annotated
        if (!annotationMap.containsKey(loincId)) {
            throw new LoincCodeNotAnnotatedException();
        }

        String loincScale = loincEntryMap.get(loincId).getScale();

        // placeholder of interpretation code
        String code = null;

        // handle special cases
        // for Qn tests without "abnormally low" (lower is better), or without "abnormally high" (higher is better
        // We will use the flag field: if abnormal, we map to H or L
        // otherwise, we map to the N field
        if (qnNoLow.contains(loincId)) {
            code = noLowQn(labEvent).orElse(null);
        } else if (qnNoHigh.contains(loincId)){
            code = noHighQn(labEvent).orElse(null);
        // process Qn tests: map to L, H or N
        } else if (loincScale.equals("Qn")) { // general cases
            code = qnLab2hpo(labEvent, labSummary).orElse(null);
        // process Ord tests
        // map to NEG or POS
        } else if (loincScale.equals("Ord")) {
            code = ordLab2hpo(labEvent, labSummary).orElse(null);
        } else {
        // do nothing for Nom types for now
        }


        if (code == null) {
            throw new UnableToInterpretException();
        } else {
            Code interpretation = new Code(new Coding().setSystem("FHIR").setCode(code));
            LOINC2HpoAnnotationImpl annotation = annotationMap.get(loincId);
            if (annotation.getCandidateHpoTerms().containsKey(interpretation)) {
                return Optional.of(annotation.getCandidateHpoTerms().get(interpretation));
            } else {
                throw new UnrecognizedCodeException("Interpretation code is not mapped to hpo");
            }
        }

    }

    private Optional<String> qnLab2hpo(LabEvent labEvent, LabSummary labSummary) {
    	// See if the event has enough information without using the summary
		if (StringUtils.isNoneEmpty(labEvent.getRefLow(), labEvent.getRefHigh(), labEvent.getValueNum() == null?null:labEvent.getValueNum().toString())) {
			try {
				Double low = Double.parseDouble(labEvent.getRefLow());
				Double high = Double.parseDouble(labEvent.getRefHigh());
				if (labEvent.getValueNum() < low) {
					return Optional.of("L");
				} else if (labEvent.getValueNum() > high) {
					return Optional.of("H");
				}
			} catch (NumberFormatException e) {
				// Oh well, we tried. Continue, using the labSummary if it exists
			}
		}
		
		if (labSummary == null) {
			return Optional.empty();
		}
     	
    	String primaryUnit = labSummary.getPrimaryUnit();
        String observedUnit = labEvent.getValueUom();
        if (observedUnit.equals(primaryUnit)) {
            return qnWithPrimaryUnit(labEvent, labSummary);
        } else {
            return qnNonPrimaryUnit(labEvent, labSummary);
        }
    }

    private Optional<String> ordLab2hpo(LabEvent labEvent, LabSummary labSummary) {
        //System.out.println(labEvent.getItem_id() + "\t" + labEvent.getValue() + "\t" + labEvent.getValue_num() + "\t" + labEvent.getFlag());
        //we try to parse the value
        Optional<String> code = ordTextualResult(labEvent);
        if (!code.isPresent() && labEvent.getValueNum() != null && labSummary != null) {
            System.out.println("Ord having numeric result: " + labEvent.getItemId() + "\t" + labEvent.getValue() + "\t" + labEvent.getValueNum() + "\t" + labEvent.getFlag());
            return Optional.empty();
        }
        return code;
    }

    // This was MIMIC-III specific
//    private Optional<String> qnTextualResult(LabEvent labEvent) {
//        String code = null;
//        String value = labEvent.getValue();
//        // corner case: this is LOINC:778-1 Platelets [#/volume] in Blood by Manual Count
//        if (labEvent.getItemId() == 51266) {
//            if (value.contains("HIGH")) {
//                code = "H";
//            } else if (value.contains("LOW") || value.contains("RARE")) {
//                code = "L";
//            } else {
//                code = null;
//            }
//        }
//        // corner case: this is LOINC:33914-3 Glomerular filtration rate/​1.73 sq M.predicted [Volume Rate/​Area]
//        // in Serum or Plasma by Creatinine-based formula (MDRD)
//        // there is no way to process this one because the value is "See comments" and comments do not exist in the dataset
//        else if (labEvent.getItemId() == 50920) {
//            // do nothing
//        }
//
//        if (code == null) {
//            return Optional.empty();
//        } else {
//            return Optional.of(code);
//        }
//    }

    private Optional<String> ordTextualResult(LabEvent labEvent) {
        String value = labEvent.getValue();
        String code = null;
        if (value.toUpperCase().matches("^NORMAL|^NEG*")) {
            code = "NEG";
        } else if (value.toUpperCase().matches("^POS*")) {
            code = "POS";
        } else {
            code = null;
        }

        return (code == null) ? Optional.empty() : Optional.of(code);
    }

    private Optional<String> qnWithPrimaryUnit(LabEvent labEvent, LabSummary labSummary) {
        String unit = labEvent.getValueUom();
        LabSummary.NormalRange normalRange = labSummary.getNormalRangeByUnit().get(unit);
        Double observed_value = labEvent.getValueNum();
        if (normalRange == null || observed_value == null) {
            return Optional.empty();
        }
        Double min_normal = normalRange.getMin();
        Double max_normal = normalRange.getMax();
        String flag = labEvent.getFlag();
        String code = null;

        if (observed_value < min_normal) {
            code = "L";
        } else if (observed_value > max_normal) {
            code = "H";
        } else {
            code = "N";
        }

        //need to adjust the result if the result was flagged as "abnormal" and we assigned to "N"
        //this happens probably due to different thresholds used by different clinical labs
        //when we combine everyone's result together, a result is now interpreted differently
        //we should try to match the original result as best as we can
        if (code.equals("N") && flag.toLowerCase().contains("abnormal")) {
            //when mismatches happen, we compare whether the value is closer to min or max
//            if (Math.abs(observed_value - min_normal) < Math.abs(observed_value - max_normal)) {
//                code = "L";
//            } else {
//                code = "H";
//            }

            //the second approach is to compare the value with global mean
            if (observed_value < labSummary.getMeanByUnit().get(labSummary.getPrimaryUnit())) {
                code = "L";
            } else {
                code = "H";
            }
        }

        return Optional.of(code);
    }

    private Optional<String> qnNonPrimaryUnit(LabEvent labEvent, LabSummary labSummary) {
        //we skip such tests for now
        return Optional.empty();
    }

    public Set<Integer> getFailedQnWithTextResult() {
        return this.failedQnWithTextResult;
    }

    private Optional<String> noLowQn(LabEvent labEvent) {
        if (labEvent.getFlag().toLowerCase().contains("abnormal")) {
            return Optional.of("H");
        } else if (labEvent.getFlag().toLowerCase().contains("normal")){
            return Optional.of("N");
        } else {
        	return Optional.empty();
        }
    }

    private Optional<String> noHighQn(LabEvent labEvent) {
        if (labEvent.getFlag().toLowerCase().contains("abnormal")) {
            return Optional.of("L");
        } else if (labEvent.getFlag().toLowerCase().contains("normal")) {
            return Optional.of("N");
        } else {
        	return Optional.empty();
        }
    }
}