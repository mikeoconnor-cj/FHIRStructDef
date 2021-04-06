USE WAREHOUSE dev_humana;
USE DATABASE dev_humana;


SELECT * FROM ods.BB_EOB_PROCEDURE --no data
SELECT * FROM ods.BB_PATIENT
SELECT * FROM ods.BB_PATIENT_LINK 
SELECT * FROM ods.BB_PATIENT_IDENTIFIER 
SELECT * FROM ods.BB_PATIENT_EXTENSION 
SELECT * FROM ods.BB_COVERAGE 

--fac_rev filtering

SELECT 'fac_rev' AS activity_type_cd
	, 'fac_rev'||'|'|| bb_eob_item.pk_eob_item_id AS pk_activity_id
	, bb_eob.src_billable_period_start AS activity_from_date
	, bb_eob.src_billable_period_end AS activity_thru_date
	, to_char(bb_eob.src_billable_period_start, 'm-YYYY-MM') AS activity_from_month_cd	
	, to_char(bb_eob.src_billable_period_end, 'm-YYYY-MM') AS activity_thru_month_cd
	, CASE
          WHEN bb_eob.src_type IN ('10','20','30','50','60','61')
          	THEN 
          	'fac'||'|'||'012345'||'|'||'0123456789'||'|'||SRC_PATIENT_REFERENCE||'|'||to_char(bb_eob.src_billable_period_start, 'm-YYYY-MM')||'|'||to_char(bb_eob.src_billable_period_end, 'm-YYYY-MM')
		ELSE
            '#NA'
      END AS fk_ip_stay_id
 	, CASE
          WHEN bb_eob.src_type IN ('10','20','30','50','60','61')
              THEN bb_eob.src_billable_period_start
          ELSE
              NULL
      END AS ip_stay_from_dt     
	, CASE
          WHEN bb_eob.src_type IN ('10','20','30','50','60','61')
              THEN bb_eob.src_billable_period_end
          ELSE
              NULL
      END AS ip_stay_thru_dt
	, CASE
          WHEN bb_eob.src_type IN ('10','20','30','50','60','61')
              THEN TO_CHAR(bb_eob.src_billable_period_start, 'm-YYYY-MM')
          ELSE
              NULL
      END AS ip_stay_from_month_cd      
	, CASE
          WHEN bb_eob.src_type IN ('10','20','30','50','60','61')
              THEN TO_CHAR(bb_eob.src_billable_period_end, 'm-YYYY-MM')
          ELSE
              NULL
      END AS ip_stay_thru_month_cd   
	, CASE
          WHEN bb_eob.src_type = '40'
              THEN 'fac'||'|'||'012345'||'|'||'0123456789'||'|'||src_patient_reference||'|'||TO_CHAR(bb_eob.src_billable_period_start,'YYYYMMDD')||'|'||TO_CHAR(bb_eob.src_billable_period_end,'YYYYMMDD')
          ELSE
              '#NA'
      END AS fk_visit_id
	, CASE
          WHEN bb_eob.src_type = '40'
              THEN bb_eob.src_billable_period_start
          ELSE
              NULL
      END AS visit_from_dt
	, CASE
          WHEN bb_eob.src_type = '40'
              THEN bb_eob.src_billable_period_end
          ELSE
              NULL
      END AS visit_thru_dt
    , CASE
          WHEN bb_eob.src_type = '40'
              THEN TO_CHAR(bb_eob.src_billable_period_start,'m-YYYY-MM')
          ELSE
              NULL
      END AS visit_from_month_cd
    , CASE
          WHEN bb_eob.src_type = '40'
              THEN TO_CHAR(bb_eob.src_billable_period_end,'m-YYYY-MM')
          ELSE
              NULL
      END AS visit_thru_month_cd
    , bb_eob.src_patient_reference AS fk_patient_id
	--, COALESCE('ccn_num'||'|'||c.src_prvdr_oscar_num, '#NA') AS fk_facility_id 
	, COALESCE('ccn_num'||'|'||'012345', '#NA') AS fk_facility_id    
--	, c.src_prvdr_oscar_num AS facility_ccn_num
	, '012345' AS facility_ccn_num
	--, h.src_fac_prvdr_npi_num AS facility_npi_num
	, '0123456789' AS facility_npi_num	
--, h.src_clm_bill_fac_type_cd AS facility_type_cd
	, '7' AS facility_type_cd		
--, h.src_clm_bill_clsfctn_cd AS facility_classification_cd
	--SELECT facility_type_cd, facility_classification_cd FROM PROD_A1052_FE.INSIGHTS.ACTIVITY WHERE LOAD_PERIOD = 'm-2021-02' AND ACTIVITY_TYPE_CD = 'fac_rev'
	--, src_subtype
	, '1' AS facility_classification_cd
	, '#NA' AS facility_place_of_service_cd
--    , 'ccn_num'||'|'||c.src_prvdr_oscar_num||'|'||c.src_clm_line_prod_rev_ctr_cd AS fk_facility_rev_ctr_id
	--is this the revenue code? yes
	--SELECT FK_FACILITY_REV_CTR_ID, facility_revenue_center_cd FROM PROD_A1052_FE.INSIGHTS.ACTIVITY WHERE LOAD_PERIOD = 'm-2021-02' AND ACTIVITY_TYPE_CD = 'fac_rev'
	, 'ccn_num'||'|'||'012345'||'|'||bb_eob_item.src_revenue AS fk_facility_rev_ctr_id    
--    , c.src_clm_line_prod_rev_ctr_cd AS facility_revenue_center_cd
	, bb_eob_item.src_revenue AS facility_revenue_center_cd
    , '#NA' AS fk_tin_id
    , '#NA' AS fk_tin_rendering_id	
	
	
	
	, bb_eob.eff_start_dt
	, bb_eob.eff_end_dt
	, bb_eob.src_created
	, src_type
	, bb_eob.ORG_ID 
	--, count(*) AS rwCnt
	, bb_eob_item.src_revenue --map to facility_revenue_center_cd 
	, bb_eob_item.src_service
	, bb_eob.SRC_PROVIDER_REFERENCE 
	, bb_eob.SRC_FACILITY_REFERENCE 
	, bb_eob.SRC_ORGANIZATION_REFERENCE 
	, bb_eob_item.src_location_reference
	, bb_eob.src_patient_reference
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type IN ('10'  --HHA
							, '20' --SNF
							, '30' --SNF
							,'40' --Outpatient
							,'50' --Hospice
							,'60' --Inpatient
							,'61' --Inpatient
							) 
	AND bb_eob_item.src_revenue IS NOT NULL
	
	--none of these. are they all icd9? they look like cpt's five digit
	AND bb_eob_item.src_service IS NOT NULL
	AND bb_eob_item.src_service IN ('30233N1', '02HV33Z', '5A09357') --took theese from mdpcp

	
SELECT min(src_serviced_date) minssd
	, max(src_serviced_date) masssd
	, min(src_serviced_period_start)	 minsps
	, max(src_serviced_period_start)  maxsps
	, min(src_serviced_period_end)	 minspe
	, max(src_serviced_period_end)  maxspe	
	, min(src_billable_period_start) minbps
	, max(src_billable_period_start) maxbps
	, min(src_billable_period_end)	minbpe
	, max(src_billable_period_end) maxbps
	, count(*) AS rwCnt
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type IN (
							'10'  --HHA     --no dates
							, '20' --SNF
							, '30' --SNF
							,'40' --Outpatient
							,'50' --Hospice
							,'60' --Inpatient
							,'61' --Inpatient
							
							,'PDE'   -- part d events   --no dates							
							
													
--							'71'   --sps, spe

--							'72'	--sps, spe
--							,'81'
--							,'82'
							
				
							) 	
	
--there are length 7 and there are no such codes in BB samplees	
	
SELECT max(len(bb_eob_item.src_service )) maxLen --11
	, min(len(bb_eob_item.src_service )) minLen --1
FROM dev_humana.ods.bb_eob_item
WHERE bb_eob_item.record_status_cd = 'a'

SELECT *
FROM dev_humana.ods.bb_eob_item
WHERE bb_eob_item.record_status_cd = 'a'
AND len(bb_eob_item.src_service ) = 5

SELECT *
FROM dev_humana.ods.bb_eob_item
WHERE bb_eob_item.record_status_cd = 'a'
AND len(bb_eob_item.src_service ) = 11

SELECT bb_eob.src_type  --these are all PDE, so maybe these are NDC codes
	, count(*) AS rwCnt
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND len(bb_eob_item.src_service ) = 11
GROUP BY src_type

SELECT bb_eob.src_type  --these are all PDE
	, count(*) AS rwCnt
FROM DEV_GEISINGER.ods.bb_eob
JOIN DEV_GEISINGER.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND len(bb_eob_item.src_service ) = 11
GROUP BY src_type



SELECT bb_eob.src_type  --31 outpatient, 2 carrier
	, count(*) AS rwCnt
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND len(bb_eob_item.src_service ) BETWEEN 3 AND 4
GROUP BY src_type

SELECT bb_eob.src_type
	, bb_eob_item.src_service
	, bb_eob.pk_eob_id
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND len(bb_eob_item.src_service ) BETWEEN 3 AND 4

	
	
SELECT *
FROM dev_humana.ods.BB_EOB_ITEM_DETAIL 

SELECT src_type, --rxcinv, rxdinv
	 , count(*)
FROM dev_humana.ods.BB_EOB_ITEM_DETAIL 
WHERE RECORD_STATUS_CD = 'a'
GROUP BY src_type

WHERE FK_EOB_ID = 'cms_mssp|outpatient-1863245665'
	
--no records at all
SELECT * 
FROM dev_humana.ods.BB_EOB_ADD_ITEM 

SELECT * 
FROM dev_humana.ods.BB_EOB_ADD_ITEM_detail


WHERE FK_EOB_ID = 'cms_mssp|outpatient-1863245665'




SELECT bb_eob.src_type  --none
	, count(*) AS rwCnt
FROM DEV_GEISINGER.ods.bb_eob
JOIN DEV_GEISINGER.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND len(bb_eob_item.src_service ) BETWEEN 3 AND 4
GROUP BY src_type

SELECT * FROM DEV_GEISINGER.ods.BB_EOB_PROCEDURE 
	
--33 roww
SELECT *
FROM dev_humana.ods.bb_eob_item
WHERE bb_eob_item.record_status_cd = 'a'
AND len(bb_eob_item.src_service ) BETWEEN 3 AND 4

		 --= '71' --also nope		
--	AND EXISTS -- no rows
--	(
--		SELECT 1
--		FROM dev_humana.ods.bb_eob_procedure
--		WHERE bb_eob.pk_eob_id = bb_eob_procedure.fk_eob_id
--		AND bb_eob_procedure.record_status_cd = 'a'
--	)
	--AND bb_eob_item.src_service IS NOT null
	
--GROUP BY src_type
--ORDER BY src_type

--fac_proc filtering
	--no records returning with this filter

SELECT 'fac_proc' AS activity_type_cd
	, src_type
	, bb_eob.ORG_ID 
	--, count(*) AS rwCnt
	, bb_eob_item.src_revenue --map to facility_revenue_center_cd 
	, bb_eob_item.src_service
	, bb_eob.SRC_PROVIDER_REFERENCE 
	, bb_eob.SRC_FACILITY_REFERENCE 
	, bb_eob.SRC_ORGANIZATION_REFERENCE 
	, bb_eob_item.src_location_reference
	, bb_eob.src_patient_reference
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type IN ('10'  --HHA
							, '20' --SNF
							, '30' --SNF
							,'40' --Outpatient
							,'50' --Hospice
							,'60' --Inpatient
							,'61' --Inpatient
							) 
	AND bb_eob_item.src_revenue IS null	
	
	
--phys filtering

	
WITH eobid 
AS 
(
SELECT fk_eob_id
	, src_value
	, SRC_SYSTEM 
FROM ods.BB_EOB_IDENTIFIER 
where RECORD_STATUS_CD = 'a'
)
, eobidPvt
AS
(
SELECT *	
FROM eobid
	pivot(max(src_value) FOR src_system IN (
	'https://bluebutton.cms.gov/resources/variables/clm_id'  --pde claim don't populate this?
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	)
	) AS p (fk_eob_id, clm_id, claim_group)	
)
SELECT 'phys' AS activity_type_cd
	, src_type
	, bb_eob.ORG_ID 
	, 'phys'||'|'||bb_eob.pk_eob_id AS pk_activity_id
	, 'phys' AS activity_type_cd
--	, coalesce(bb_eob_item.src_serviced_period_start,bb_eob.src_billable_period_start)
--			AS activity_from_dt
	, bb_eob_item.src_serviced_period_start AS activity_from_dt
--	, coalesce(bb_eob_item.src_serviced_period_end,bb_eob.src_billable_period_end)
--			AS activity_thru_dt
	, bb_eob_item.src_serviced_period_end AS activity_thru_dt			
--	, to_char(coalesce(bb_eob_item.src_serviced_period_start,bb_eob.src_billable_period_start),'m-YYYY-MM')		
--			AS activity_from_month_cd
	, to_char(bb_eob_item.src_serviced_period_start,'m-YYYY-MM')		
			AS activity_from_month_cd
--	, to_char(coalesce(bb_eob_item.src_serviced_period_end,bb_eob.src_billable_period_end),'m-YYYY-MM')
--			AS activity_thru_month_cd
	, to_char(bb_eob_item.src_serviced_period_end,'m-YYYY-MM')
			AS activity_thru_month_cd	
	, '#NA' AS fk_ip_stay_id
	, NULL AS ip_stay_from_dt
	, NULL AS ip_stay_from_month_cd	
	, NULL AS ip_stay_thru_month_cd
 	, 'phys'||'|'||eobidPvt.clm_id||'|'||eobidPvt.claim_group||'|'||COALESCE(bb_eob_care_team.src_provider_npi,'#NA')||'|'||bb_eob.src_patient_reference||'|'||to_char(bb_eob_item.src_serviced_period_start,'m-YYYY-MM')||'|'||to_char(bb_eob_item.src_serviced_period_end,'m-YYYY-MM') AS fk_visit_id
	, coalesce(bb_eob.src_billable_period_start, bb_eob_item.src_serviced_period_start) AS visit_from_dt
	, coalesce(bb_eob.src_billable_period_end, bb_eob_item.src_serviced_period_end) AS visit_thru_dt
	, to_char(coalesce(bb_eob.src_billable_period_start, bb_eob_item.src_serviced_period_start) ,'m-YYYY-MM') AS visit_from_month_cd
	, to_char(coalesce(bb_eob.src_billable_period_end, bb_eob_item.src_serviced_period_end) ,'m-YYYY-MM') AS visit_thru_month_cd
	, bb_eob.src_patient_reference AS fk_patient_id
	--COALESCE('tin'||'|'||c.src_clm_rndrg_prvdr_tax_num, '#NA') AS fk_facility_id
--	, COALESCE('npi'||'|'||bb_eob_care_team.src_provider_npi, '#NA') AS fk_facility_id
	, '#NA' AS fk_facility_id
    , '#NA' AS facility_ccn_num
    , '#NA' AS facility_npi_num
    , '#NA' AS facility_type_cd
    , '#NA' AS facility_classification_cd	  
	--, c.src_clm_pos_cd AS facility_place_of_service_cd
	, bb_eob_item.src_location_code AS facility_place_of_service_cd 
	--, 'tin_pos'||'|'||c.src_clm_rndrg_prvdr_tax_num||'|'||c.src_clm_pos_cd AS fk_facility_rev_ctr_id	
	, 'tin_pos'||'|'||'#NA'||'|'||bb_eob_item.src_location_code	AS fk_facility_rev_ctr_id
	, '#NA' AS facility_revenue_center_cd
	--, src_clm_rndrg_prvdr_tax_num AS fk_tin_id
	, '#NA' AS fk_tin_id	
    -- src_clm_rndrg_prvdr_tax_num fk_tin_rendering_id
    , '#NA' AS fk_tin_rendering_id
    
 	, bb_eob.pk_eob_id
	, bb_eob.src_claim_reference
	, bb_eob_item.src_serviced_date
	, bb_eob_item.src_serviced_period_start
	, bb_eob_item.src_serviced_period_end
	, bb_eob.src_billable_period_start
	, bb_eob.src_billable_period_end
	, bb_eob.src_created
	--, count(*) AS rwCnt
	, bb_eob_item.src_revenue --map to facility_revenue_center_cd 
	, bb_eob_item.src_service
	, bb_eob.SRC_PROVIDER_REFERENCE 
	, bb_eob.SRC_FACILITY_REFERENCE 
	, bb_eob.SRC_ORGANIZATION_REFERENCE 
	, bb_eob_item.src_location_reference
	, bb_eob_item.src_location_code  --it's 99 
	, bb_eob.src_patient_reference
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
LEFT JOIN ods.BB_EOB_CARE_TEAM   --src_provider_npi
	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
	AND bb_eob_care_team.record_status_cd = 'a'
	AND bb_eob_care_team.SRC_SEQUENCE = 2
LEFT JOIN eobidPvt
	ON bb_eob.pk_eob_id = eobidPvt.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type IN ('71','72') 

	
	
	
	
SELECT src_value
	, src_eob_id
--	, SRC_TYPE 
	, SRC_SYSTEM 
--SELECT *	
FROM ods.BB_EOB_IDENTIFIER 
where FK_EOB_ID = 'cms_mssp|carrier-10479839850'
	AND RECORD_STATUS_CD = 'a'
	AND SRC_SYSTEM LIKE '%clm_id'

https://bluebutton.cms.gov/resources/variables/clm_id	
https://bluebutton.cms.gov/resources/identifier/claim-group

SELECT fk_eob_id
	, p.*
--SELECT *	
FROM ods.BB_EOB_IDENTIFIER 
	pivot(sum(src_value) FOR src_system IN (
	'https://bluebutton.cms.gov/resources/variables/clm_id'
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	)
	) AS p
where FK_EOB_ID = 'cms_mssp|carrier-10479839850'
	AND RECORD_STATUS_CD = 'a'	
	
WITH eobid 
AS 
(
SELECT src_eob_id
	, src_value
--	, SRC_TYPE 
	, SRC_SYSTEM 
--SELECT *	
FROM ods.BB_EOB_IDENTIFIER 
where FK_EOB_ID = 'cms_mssp|carrier-10479839850'
	AND RECORD_STATUS_CD = 'a'
)
SELECT *	
FROM eobid
	pivot(max(src_value) FOR src_system IN (
	'https://bluebutton.cms.gov/resources/variables/clm_id'
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	)
	) AS p

WITH eobid 
AS 
(
SELECT src_eob_id
	, src_value
--	, SRC_TYPE 
	, SRC_SYSTEM 
--SELECT *	
FROM ods.BB_EOB_IDENTIFIER 
where FK_EOB_ID = 'cms_mssp|carrier-10479839850'
	AND RECORD_STATUS_CD = 'a'
)
SELECT *	
FROM eobid
	pivot(max(src_value) FOR src_system IN (
	'https://bluebutton.cms.gov/resources/variables/clm_id'
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	)
	) AS p (src_eob_id, clm_id, claim_group)





SELECT fk_eob_id
	, p.*
--SELECT *	
FROM ods.BB_EOB_IDENTIFIER 
	pivot(sum(src_value) FOR src_system IN (
	'https://bluebutton.cms.gov/resources/variables/clm_id'
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	)
	) AS p
where FK_EOB_ID = 'cms_mssp|carrier-10479839850'
	AND RECORD_STATUS_CD = 'a'		
	
USE WAREHOUSE dev_humana;
USE DATABASE dev_humana;
	
SELECT *
FROM ods.BB_EOB_CARE_TEAM 
WHERE 
--FK_EOB_ID = 'cms_bb|carrier-10479839850'
--'cms_mssp|carrier-10479839850'
--AND 
RECORD_STATUS_CD = 'a'
AND FK_EOB_ID LIKE '%carrier%'
AND FK_EOB_ID LIKE 'cms_bb%'  --one record, load_period 'm-2020-11'

FK_EOB_ID
cms_mssp|carrier--21731989082

--mixed dash and double dash in the pk

SELECT count(*) --602,970 OUT OF 1,756,090
SELECT bb_eob_care_team.*
FROM ods.bb_eob 
JOIN ods.BB_EOB_CARE_TEAM 
	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
WHERE 

	bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_care_team.record_status_cd = 'a'
--AND PK_EOB_ID LIKE 'cms_bb%'  --none of these
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type IN ('71','72') 
	AND bb_eob_care_team.SRC_SEQUENCE = 2 --602,970


SELECT *  --no reecords
FROM DEV_HUMANA.ods.bb_eob
WHERE RECORD_STATUS_CD = 's'	
	AND SRC_TYPE IN ('81','82')	

SELECT *  --no reecords
FROM DEV_GEISINGER.ods.bb_eob
WHERE RECORD_STATUS_CD = 's'	
	AND SRC_TYPE IN ('81','82')		

	
--dme filteering
	--I don't see any dme data
	
SELECT 'dme' AS activity_type_cd
	, src_type
	, bb_eob.ORG_ID 
	, 'dme'||'|'||bb_eob.pk_eob_id AS pk_activity_id
	, 'dme' AS activity_type_cd
--	, coalesce(bb_eob_item.src_serviced_period_start,bb_eob.src_billable_period_start)
--			AS activity_from_dt
	, bb_eob_item.src_serviced_period_start AS activity_from_dt			
--	, coalesce(bb_eob_item.src_serviced_period_end,bb_eob.src_billable_period_end)
--			AS activity_thru_dt	
	, bb_eob_item.src_serviced_period_end AS activity_thru_dt				
--	, to_char(coalesce(bb_eob_item.src_serviced_period_start,bb_eob.src_billable_period_start),'m-YYYY-MM')		
--			AS activity_from_month_cd
	, to_char(bb_eob_item.src_serviced_period_start,'m-YYYY-MM')		
			AS activity_from_month_cd			
--	, to_char(coalesce(bb_eob_item.src_serviced_period_end,bb_eob.src_billable_period_end),'m-YYYY-MM')
--			AS activity_thru_month_cd
	, to_char(bb_eob_item.src_serviced_period_end,'m-YYYY-MM')
			AS activity_thru_month_cd			
	--, count(*) AS rwCnt
	, '#NA' AS fk_ip_stay_id
	, NULL AS ip_stay_from_dt
	, NULL AS ip_stay_thru_dt
	, NULL AS ip_stay_from_month_cd
	, NULL AS ip_stay_thru_month_cd
	, '#NA' AS fk_visit_id
    , NULL AS visit_from_dt
    , NULL AS visit_thru_dt
    , '#NA' AS visit_from_month_cd
    , '#NA' AS visit_thru_month_cd
	, bb_eob.src_patient_reference AS fk_patient_id	
	, '#NA' AS fk_facility_id
    , '#NA' AS facility_ccn_num	   
	, '#NA' AS facility_npi_num
    , '#NA' AS facility_type_cd
    , '#NA' AS facility_classification_cd
 	, '#NA' AS fk_facility_rev_ctr_id
    , '#NA' AS facility_revenue_center_cd 
    , '#NA' AS fk_tin_id
    , '#NA' AS fk_tin_rendering_id    
    
    
	--, c.src_clm_pos_cd AS facility_place_of_service_cd    
    , bb_eob_item.src_location_code AS facility_place_of_service_cd
	, bb_eob_item.src_revenue --map to facility_revenue_center_cd 
	
	
	, bb_eob_item.src_service
	, bb_eob.SRC_PROVIDER_REFERENCE 
	, bb_eob.SRC_FACILITY_REFERENCE 
	, bb_eob.SRC_ORGANIZATION_REFERENCE 
	, bb_eob_item.src_location_reference
	, bb_eob.src_patient_reference
FROM DEV_HUMANA.ods.bb_eob
JOIN DEV_HUMANA.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type in ('81'
							,'82'
	) 	
	
--med filtering

USE WAREHOUSE dev_humana;
USE DATABASE dev_humana;
	
SELECT 'med' AS activity_type_cd
	, src_type
	, bb_eob.ORG_ID 
	--, bb_eob.src_billable_period_start AS activity_from_date
	, bb_eob_item.src_serviced_date AS activity_from_date
	--, bb_eob.src_billable_period_end AS activity_thru_date	
	, bb_eob_item.src_serviced_date AS activity_thru_date	
	--, to_char(bb_eob.src_billable_period_start, 'm-YYYY-MM') AS activity_from_month_cd
	, to_char(bb_eob_item.src_serviced_date, 'm-YYYY-MM') AS activity_from_month_cd
	--, to_char(bb_eob.src_billable_period_end, 'm-YYYY-MM') AS activity_thru_month_cd
	, to_char(bb_eob_item.src_serviced_date, 'm-YYYY-MM') AS activity_thru_month_cd
	--, count(*) AS rwCnt
	, '#NA' AS fk_ip_stay_id
    , NULL AS ip_stay_from_dt
    , NULL AS ip_stay_thru_dt
    , NULL AS ip_stay_from_month_cd
    , NULL AS ip_stay_thru_month_cd
	, 'med'||'|'||bb_eob_care_team.src_provider_npi||'|'||bb_eob.src_patient_reference||'|'||
	  to_char(COALESCE(bb_eob_item.src_serviced_date,CAST('1900-01-01' AS DATE)),'YYYYMMDD') AS fk_visit_id
       --the following visit dates calculate the same as the Activity data as in Insights
 	, bb_eob_item.src_serviced_date AS visit_from_dt 	
	, bb_eob_item.src_serviced_date AS visit_thru_dt	
	, TO_CHAR(bb_eob_item.src_serviced_date,'m-YYYY-MM') AS visit_from_month_cd
 	, TO_CHAR(bb_eob_item.src_serviced_date,'m-YYYY-MM') AS visit_thru_month_cd	
--	, c.fk_bene_id AS fk_patient_id 
	, bb_eob.src_patient_reference AS fk_patient_id
	, 'dspns_prvdr'||'|'||'npi_num'||'|'||COALESCE('0123456789','#NA') AS fk_facility_id
	, '#NA' AS facility_ccn_num	
	, 'dspns_prvdr'||'|'||COALESCE('0123456789','#NA') AS facility_npi_num
    , '#NA' AS facility_type_cd
    , '#NA' AS facility_classification_cd
    , '#NA' AS facility_place_of_service_cd
    , '#NA' AS fk_facility_rev_ctr_id
    , '#NA' AS facility_revenue_center_cd
	, '#NA' AS fk_tin_id
	, '#NA' AS fk_tin_rendering_id		
	
	, bb_eob.pk_eob_id
	, bb_eob_item.src_revenue --map to facility_revenue_center_cd 
	, bb_eob_item.src_service
	, bb_eob.SRC_PROVIDER_REFERENCE 
	, bb_eob.SRC_FACILITY_REFERENCE 
	, bb_eob.SRC_ORGANIZATION_REFERENCE 
	, bb_eob_item.src_location_reference
	, bb_eob.src_patient_reference
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
LEFT JOIN ods.BB_EOB_CARE_TEAM   --src_provider_npi
	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
	AND bb_eob_care_team.record_status_cd = 'a'
	AND bb_eob_care_team.SRC_SEQUENCE = 2	
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type = 'PDE'
	
PK_EOB_ID
cms_mssp|pde-551279807
cms_mssp|pde-5914722060
cms_mssp|pde-7230314532

SELECT fk_eob_id
	, src_responsible
	, src_role
	, src_qualification
	, src_sequence
	, src_provider_npi
	, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
--SELECT * 
FROM ods.BB_EOB_CARE_TEAM
WHERE record_status_cd = 'a'
AND fk_eob_id IN (
'cms_mssp|outpatient-1772697339'
,'cms_mssp|outpatient-1163525072'
,'cms_mssp|outpatient-1774328067'
)
ORDER BY fk_eob_id






PK_EOB_CARE_TEAM_ID
cms_mssp|outpatient-1163525072|9999999999

SELECT *   ---src_sequence 4,5
FROM ods.BB_EOB_CARE_TEAM
WHERE fk_eob_id = 'cms_mssp|outpatient-1163525072'
--PK_EOB_CARE_TEAM_ID = 'cms_mssp|outpatient-1163525072|9999999999'
AND record_status_cd = 'a'
ORDER BY src_sequence 

SELECT bb_eob.pk_eob_id
	, count(*) AS recCnt
FROM ods.bb_eob
JOIN ods.BB_EOB_CARE_TEAM   --src_provider_npi
	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
WHERE bb_eob_care_team.record_status_cd = 'a'
	AND bb_eob.record_status_cd = 'a'
	AND bb_eob.src_type = 'PDE'  --no multiples
GROUP BY pk_eob_id
HAVING count(*) > 1

SELECT bb_eob.src_type
	, bb_eob.pk_eob_id
	, count(*) AS recCnt
	, min(src_sequence) AS minSrcSqnc
FROM ods.bb_eob
JOIN ods.BB_EOB_CARE_TEAM   --src_provider_npi
	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
WHERE bb_eob_care_team.record_status_cd = 'a'
	AND bb_eob.record_status_cd = 'a'
	AND bb_eob.src_type <> '40'  --no records when Not outpatient
	--= 'PDE'  --no multiples
GROUP BY bb_eob.src_type, pk_eob_id
HAVING count(*) > 1   --when row count is 1, min src sequence *is* often 2
						--when row count > 1 min src sequescee is  often 4 
AND min(src_sequence) = 2

SELECT *
FROM ods.BB_EOB_CARE_TEAM 
WHERE record_status_cd = 'a'
AND src_role <> 'primary'  --only 2 claims with src_sequence 2 and 3 the exception to usual
--2 records: 'assist', 'other'..   looks like outpatient claims
--other roles are 'primary' and 'supervisor'

--https://community.snowflake.com/s/question/0D50Z00008izVcQSAU/filter-array-elements
--https://docs.snowflake.com/en/sql-reference/functions/array_to_string.html

--array agg sorting w/in groups identifying role vs. ID type or activity_type_cd
select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as fk_provider_list
FROM ods.BB_EOB_CARE_TEAM 
WHERE record_status_cd = 'a'

--filtering with array_contains and array_construct

select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as fk_provider_list
FROM ods.BB_EOB_CARE_TEAM 
WHERE record_status_cd = 'a'
	and array_contains(src_role, array_construct('primary', 'supervisor')) = TRUE

select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as fk_provider_list
FROM ods.BB_EOB_CARE_TEAM 
WHERE record_status_cd = 'a'
	and array_contains(src_role, array_construct('assist','other','supervisor')) = FALSE