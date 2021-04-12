USE WAREHOUSE dev_humana;
USE DATABASE dev_humana;


--fac_rev filtering
--768,631

WITH allProvdrData
AS 
(
SELECT fk_eob_id
	, src_responsible
	, src_role
	, CASE 
		WHEN src_role = 'primary' THEN 1
		WHEN src_role = 'supervisor' THEN 2
		WHEN src_role = 'assist' THEN 3
		WHEN src_role = 'other' THEN 4
		ELSE 5
	  END role_value
	, src_qualification
	, src_sequence
	, src_provider_npi
	, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
FROM ods.BB_EOB_CARE_TEAM
WHERE record_status_cd = 'a'
ORDER BY fk_eob_id
	, src_sequence
)
--array agg sorting w/in groups identifying role vs. ID type (NPI) or activity_type_cd
, allPrvdrRoleLabel
AS 
(
select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty
FROM allProvdrData
GROUP BY fk_eob_id
)
, allPrvdrNPILabel
AS 
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty	
FROM allProvdrData
GROUP BY fk_eob_id
)
, primaryOnlyPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty		
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('primary')) = TRUE   
GROUP BY fk_eob_id
)
, allOtherPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty			
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('supervisor','assist','other')) = TRUE   
GROUP BY fk_eob_id
)

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
	, allPrvdrNPILabel.providerID AS fk_provider_id_list
	, to_array('npi_num'||'|'||bb_eob_care_team.src_provider_npi) AS fk_provider_id_list2
	, coalesce(trim(primaryOnlyPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_primary_id 
	--, 'npi_num'||'|'||COALESCE(h.src_oprtg_prvdr_npi_num,'#NA') AS fk_provider_operating_id
	, '#NA' AS fk_provider_operating_id	
	--, 'npi_num'||'|'||COALESCE(h.src_atndg_prvdr_npi_num,'#NA') AS fk_provider_attending_id
	, '#NA'	AS fk_provider_attending_id
	--npi_num'||'|'||COALESCE(h.src_othr_prvdr_npi_num,'#NA') AS fk_provider_other_id
	, coalesce(trim(allOtherPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_other_id 
	, '#NA' AS fk_provider_rendering_id
    , '#NA' AS provider_rendering_specialty_cd
    , '#NA' AS provider_rendering_type_cd
    , '#NA' AS fk_provider_pay_to_id
    , '#NA' AS fk_provider_ordering_id
    , '#NA' AS fk_provider_dispensing_id
    , '#NA' AS provider_dispensing_id_type_cd
    , '#NA' AS fk_provider_prescribing_id
    , '#NA' AS provider_prescribing_id_type_cd	
	, TO_ARRAY('#NA') AS fk_diagnosis_id_list
	, TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_9_cd_list
	, TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_10_cd_list	
	, 'hcpcs_cd'||'|'||COALESCE(bb_eob_item.src_service,'#NA') AS fk_procedure_id
	, COALESCE(xref.target_1_value, '#NA') AS procedure_betos_cd
	, COALESCE(bb_eob_item.src_service,'#NA') AS procedure_hcpcs_cd
	, ARRAY_CONSTRUCT(
            COALESCE(bb_eob_item.src_modifier[0].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[1].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[2].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[3].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[4].coding[0]['code'],'#NA')
      ) AS procedure_hcpcs_mod_cd_list
	, '#NA' AS procedure_icd_9_cd
    , '#NA' AS procedure_icd_10_cd      
	--, COALESCE(h.src_clm_op_srvc_type_cd,'#NA') AS service_op_type_cd
	-- bb standard favours carries claims for below field
	, '#NA' AS service_op_type_cd
	, '#NA' AS service_cms_type_cd
	-- c.src_clm_line_srvc_unit_qty AS service_units
	, bb_eob_item.src_quantity AS service_units
	
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
LEFT JOIN allPrvdrNPILabel
	ON bb_eob.pk_eob_id = allPrvdrNPILabel.fk_eob_id
--alternative  ... use careTeamLinkID[0] and pick the one provider
		--why is careTeamLinkID an array?
LEFT JOIN ods.BB_EOB_CARE_TEAM 
	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
	AND bb_eob_item.src_care_Team_Link_ID[0] = bb_eob_care_team.src_sequence
	AND bb_eob_care_team.record_status_cd = 'a'
LEFT JOIN primaryOnlyPrvdrNPILabel
	ON bb_eob.pk_eob_id = primaryOnlyPrvdrNPILabel.fk_eob_id
LEFT JOIN allOtherPrvdrNPILabel
	ON bb_eob.pk_eob_id = allOtherPrvdrNPILabel.fk_eob_id
LEFT JOIN dev_common.ref.code_xref_map xref
    ON bb_eob_item.src_service = xref.source_1_value
        AND xref.xref_id = 'hcpcs_x_betos'	
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
													

	--AND bb_eob_item.src_revenue IS NOT NULL  --there's no diff in # of rows retrieved



--fac_proc filtering
	--no records returning with this filter
	--corrrect filtering likely relies on 
	--data being populated in bb_eob_procedure, currently not populated

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

--alternative

WITH allProvdrData
AS 
(
SELECT fk_eob_id
	, src_responsible
	, src_role
	, CASE 
		WHEN src_role = 'primary' THEN 1
		WHEN src_role = 'supervisor' THEN 2
		WHEN src_role = 'assist' THEN 3
		WHEN src_role = 'other' THEN 4
		ELSE 5
	  END role_value
	, src_qualification
	, src_sequence
	, src_provider_npi
	, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
FROM ods.BB_EOB_CARE_TEAM
WHERE record_status_cd = 'a'
ORDER BY fk_eob_id
	, src_sequence
)
--array agg sorting w/in groups identifying role vs. ID type (NPI) or activity_type_cd
, allPrvdrRoleLabel
AS 
(
select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty
FROM allProvdrData
GROUP BY fk_eob_id
)
, allPrvdrNPILabel
AS 
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty	
FROM allProvdrData
GROUP BY fk_eob_id
)	
, primaryOnlyPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty		
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('primary')) = TRUE   
GROUP BY fk_eob_id
)
, allOtherPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty			
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('supervisor','assist','other')) = TRUE   
GROUP BY fk_eob_id
)	
SELECT '{{dag_run.conf.org_id}}' AS org_id    --'HUMANA'
	, 'fac_proc'||'|'|| bb_eob_procedure.pk_eob_procedure_id AS pk_activity_id
	, 'fac_proc' AS activity_type_cd
	, bb_eob_procedure.src_date AS activity_from_dt
	, bb_eob_procedure.src_date AS activity_thru_dt
	, TO_CHAR(bb_eob_procedure.src_date,'m-YYYY-MM') AS activity_from_month_cd
	, TO_CHAR(bb_eob_procedure.src_date,'m-YYYY-MM') AS activity_thru_month_cd
	, CASE
          WHEN bb_eob.src_type IN ('10','20','30','50','60','61')
              THEN 'fac'||'|'||'012345'||'|'||'0123455689'||'|'||bb_eob.src_patient_reference||'|'||TO_CHAR(bb_eob.src_billable_period_start,'YYYYMMDD')||'|'||TO_CHAR(bb_eob.src_billable_period_end,'YYYYMMDD')
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
              THEN 'fac_proc'||'|'||'012345'||'|'||'0123456789'||'|'||bb_eob.src_patient_reference||'|'||TO_CHAR(bb_eob.src_billable_period_start,'YYYYMMDD')||'|'||TO_CHAR(bb_eob.src_billable_period_end,'YYYYMMDD')
          ELSE
              '#NA'
      END AS fk_visit_id 
	, CASE
          WHEN bb_eob.src_type  = '40'
              THEN bb_eob.src_billable_period_start
          ELSE
              NULL
      END AS visit_from_dt      
	, CASE
          WHEN bb_eob.src_type IN ('10','40') --why '10' too..error? 
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
	, COALESCE('ccn_num'||'|'||'012345', '#NA') AS fk_facility_id  
	, '012345' AS facility_ccn_num	
	, '0123456789' AS facility_npi_num	
	, '7' AS facility_type_cd	
	, '1' AS facility_classification_cd	
	, '#NA' AS facility_place_of_service_cd
    , '#NA' AS fk_facility_rev_ctr_id
    , '#NA' AS facility_revenue_center_cd
    , '#NA' AS fk_tin_id
    , '#NA' AS fk_tin_rendering_id	
	, allPrvdrNPILabel.providerID AS fk_provider_id_list
	--, to_array('npi_num'||'|'||bb_eob_care_team.src_provider_npi) AS fk_provider_id_list2		
	, coalesce(trim(primaryOnlyPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_primary_id 
	, '#NA' AS fk_provider_operating_id 
	, '#NA' AS fk_provider_attending_id
	, coalesce(trim(allOtherPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_other_id 
	, '#NA' AS fk_provider_rendering_id
    , '#NA' AS provider_rendering_specialty_cd
    , '#NA' AS provider_rendering_type_cd
    , '#NA' AS fk_provider_pay_to_id
    , '#NA' AS fk_provider_ordering_id
    , '#NA' AS fk_provider_dispensing_id
    , '#NA' AS provider_dispensing_id_type_cd
    , '#NA' AS fk_provider_prescribing_id
    , '#NA' AS provider_prescribing_id_type_cd   
	, TO_ARRAY('#NA') AS fk_diagnosis_id_list
    , TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_9_cd_list
    , TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_10_cd_list    	
	, 'icd_10_pcs_cd'||'|'||bb_eob_procedure.src_procedure_code AS fk_procedure_id
	, '#NA' AS procedure_betos_cd
    , '#NA' AS procedure_hcpcs_cd
    , TO_ARRAY('#NA') AS procedure_hcpcs_mod_cd_list
	, '#NA' AS procedure_icd_9_cd
	, bb_eob_procedure.src_procedure_code AS procedure_icd_10_cd
	, '#NA' AS service_op_type_cd	
	, '#NA' AS service_cms_type_cd
	, 0.0 AS service_units	
	
FROM DEV_HUMANA.ods.bb_eob
JOIN dev_humana.ods.BB_EOB_PROCEDURE 
	ON bb_eob.pk_eob_id = bb_eob_procedure.fk_eob_id
LEFT JOIN allPrvdrNPILabel
	ON bb_eob.pk_eob_id = allPrvdrNPILabel.fk_eob_id
--alternative  ... use careTeamLinkID[0] and pick the one provider
		--BB Standards regarding requirement for careTeamLinkID within Item
		--is inconsistent with data provided in BB sandbox and dev_humana 
		--around Carrier and Inpatient EOBs
			--but I don't go to bb_eob_item in this query
--LEFT JOIN ods.BB_EOB_CARE_TEAM 
--	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
--	AND bb_eob_item.src_care_Team_Link_ID[0] = bb_eob_care_team.src_sequence
--	AND bb_eob_care_team.record_status_cd = 'a'
LEFT JOIN primaryOnlyPrvdrNPILabel
	ON bb_eob.pk_eob_id = primaryOnlyPrvdrNPILabel.fk_eob_id
LEFT JOIN allOtherPrvdrNPILabel
	ON bb_eob.pk_eob_id = allOtherPrvdrNPILabel.fk_eob_id	
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_procedure.record_status_cd = 'a'
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
	--AND bb_eob_item.src_revenue IS null	




--phys filtering
--3,248,492
	
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
	'https://bluebutton.cms.gov/resources/variables/clm_id'  --pde claims don't populate this?
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	)
	) AS p (fk_eob_id, clm_id, claim_group)	
)
, allProvdrData
AS 
(
SELECT fk_eob_id
	, src_responsible
	, src_role
	, CASE 
		WHEN src_role = 'primary' THEN 1
		WHEN src_role = 'supervisor' THEN 2
		WHEN src_role = 'assist' THEN 3
		WHEN src_role = 'other' THEN 4
		ELSE 5
	  END role_value
	, src_qualification
	, src_sequence
	, src_provider_npi
	, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
FROM ods.BB_EOB_CARE_TEAM
WHERE record_status_cd = 'a'
ORDER BY fk_eob_id
	, src_sequence
)
--array agg sorting w/in groups identifying role vs. ID type (NPI) or activity_type_cd
, allPrvdrRoleLabel
AS 
(
select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty
FROM allProvdrData
GROUP BY fk_eob_id
)
, allPrvdrNPILabel
AS 
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty	
FROM allProvdrData
GROUP BY fk_eob_id
)
, primaryOnlyPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty		
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('primary')) = TRUE   
GROUP BY fk_eob_id
)
, allOtherPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty			
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('supervisor','assist','other')) = TRUE   
GROUP BY fk_eob_id
)
, eobDiagnosis
AS
(
	SELECT fk_eob_id
		, src_diagnosis_code
		, src_diagnosis_reference
		, src_type
		, src_package_code  --is this the drg? repeated for each item?
		, src_sequence
		, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
	FROM ods.bb_eob_diagnosis
	where record_status_cd = 'a'
	ORDER BY fk_eob_id
		, src_sequence 

)
, allEOBDxCodeAgg
AS 
(
	SELECT fk_eob_id
	, array_agg('icd_10_cm' || '|' || src_diagnosis_code) within group(order by src_sequence) AS fkDiagCode
	, array_agg(src_diagnosis_code) within group(order by src_sequence) AS diagCode
	FROM eobDiagnosis 
	GROUP BY fk_eob_id
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
-- 	, 'phys'||'|'||eobidPvt.clm_id||'|'||eobidPvt.claim_group||'|'||COALESCE(bb_eob_care_team.src_provider_npi,'#NA')||'|'||bb_eob.src_patient_reference||'|'||to_char(bb_eob_item.src_serviced_period_start,'m-YYYY-MM')||'|'||to_char(bb_eob_item.src_serviced_period_end,'m-YYYY-MM') AS fk_visit_id
 	, 'phys'||'|'||eobidPvt.clm_id||'|'||eobidPvt.claim_group||'|'||COALESCE(split_part(primaryOnlyPrvdrNPILabel.providerID[0],'|',2),'#NA')||'|'||bb_eob.src_patient_reference||'|'||to_char(bb_eob_item.src_serviced_period_start,'m-YYYY-MM')||'|'||to_char(bb_eob_item.src_serviced_period_end,'m-YYYY-MM') AS fk_visit_id

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
    , COALESCE(primaryOnlyPrvdrNPILabel.providerID,array_construct('#NA')) AS fk_provider_id_list   
    --'npi_num'||'|'||c.src_rndrg_prvdr_npi_num AS fk_provider_primary_id
	, trim(primaryOnlyPrvdrNPILabel.providerID[0]) AS fk_provider_primary_id    
 	, '#NA' AS fk_provider_operating_id
    , '#NA' AS fk_provider_attending_id
    , '#NA' AS fk_provider_other_id 
    , trim(primaryOnlyPrvdrNPILabel.providerID[0]) AS fk_provider_rendering_id 
	--c.src_clm_prvdr_spclty_cd AS provider_rendering_specialty_cd    
	, trim(primaryOnlyPrvdrNPILabel.providerSpclty[0]) AS provider_rendering_speecialty_cd  
	--c.src_rndrg_prvdr_type_cd AS provider_rendering_type_cd	
	, trim(primaryOnlyPrvdrNPILabel.providerRole[0]) AS provider_rendering_type_cd
	
	, '#NA' AS fk_provider_pay_to_id
    , '#NA' AS fk_provider_ordering_id
    , '#NA' AS fk_provider_dispensing_id
    , '#NA' AS provider_dispensing_id_type_cd
    , '#NA' AS fk_provider_prescribing_id
    , '#NA' AS provider_prescribing_id_type_cd	
	, allEOBDxCodeAgg.fkDiagCode AS fk_diagnosis_id_list
	, TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_9_cd_list
	, allEOBDxCodeAgg.diagCode AS diagnosis_provider_detail_icd_10_cd_list	
	, COALESCE('hcpcs_cd'||'|'||bb_eob_item.src_service,'#NA') AS fk_procedure_id
	, COALESCE(xref.target_1_value, '#NA') AS procedure_betos_cd
	, COALESCE(bb_eob_item.src_service,'#NA') AS procedure_hcpcs_cd
	, ARRAY_CONSTRUCT(
            COALESCE(bb_eob_item.src_modifier[0].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[1].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[2].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[3].coding[0]['code'],'#NA')
          , COALESCE(bb_eob_item.src_modifier[4].coding[0]['code'],'#NA')
      ) AS procedure_hcpcs_mod_cd_list
	, '#NA' AS procedure_icd_9_cd
    , '#NA' AS procedure_icd_10_cd
    , '#NA' AS service_op_type_cd
	, bb_eob_item.src_category AS service_cms_type_cd
	, 0.0 AS service_units	
	
    , bb_eob_item.src_category
	, bb_eob_item.src_modifier
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
--LEFT JOIN ods.BB_EOB_CARE_TEAM   --src_provider_npi
--	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
--	AND bb_eob_care_team.record_status_cd = 'a'
--	AND bb_eob_care_team.SRC_SEQUENCE = 2
LEFT JOIN primaryOnlyPrvdrNPILabel
	ON bb_eob.pk_eob_id = primaryOnlyPrvdrNPILabel.fk_eob_id
LEFT JOIN eobidPvt
	ON bb_eob.pk_eob_id = eobidPvt.fk_eob_id
LEFT JOIN allEOBDxCodeAgg
	ON bb_eob.pk_eob_id = allEOBDxCodeAgg.fk_eob_id
LEFT JOIN dev_common.ref.code_xref_map xref
    ON bb_eob_item.src_service = xref.source_1_value
        AND xref.xref_id = 'hcpcs_x_betos'		
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type IN ('71','72') 



--dme filtering
	--I don't see any dme data, sql below doesn't pull records but there're no errors
		
WITH allProvdrData
AS 
(
SELECT fk_eob_id
	, src_responsible
	, src_role
	, CASE 
		WHEN src_role = 'primary' THEN 1
		WHEN src_role = 'supervisor' THEN 2
		WHEN src_role = 'assist' THEN 3
		WHEN src_role = 'other' THEN 4
		ELSE 5
	  END role_value
	, src_qualification
	, src_sequence
	, src_provider_npi
	, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
FROM ods.BB_EOB_CARE_TEAM
WHERE record_status_cd = 'a'
ORDER BY fk_eob_id
	, src_sequence
)
--array agg sorting w/in groups identifying role vs. ID type (NPI) or activity_type_cd
, allPrvdrRoleLabel
AS 
(
select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty
FROM allProvdrData
GROUP BY fk_eob_id
)
, allPrvdrNPILabel
AS 
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty	
FROM allProvdrData
GROUP BY fk_eob_id
)
, primaryOnlyPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty		
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('primary')) = TRUE   
GROUP BY fk_eob_id
)
, allOtherPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty			
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('supervisor','assist','other')) = TRUE   
GROUP BY fk_eob_id
)	
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
	, allPrvdrNPILabel.providerID AS fk_provider_id_list
	, coalesce(trim(primaryOnlyPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_primary_id 
	, '#NA' AS fk_provider_operating_id
    , '#NA' AS fk_provider_attending_id
    , '#NA' AS fk_provider_other_id
    , '#NA' AS fk_provider_rendering_id
    , '#NA' AS provider_rendering_specialty_cd
    , '#NA' AS provider_rendering_type_cd
	, coalesce(trim(primaryOnlyPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_pay_to_id
	, coalesce(trim(primaryOnlyPrvdrNPILabel.providerID[0]), 'npiNum|#NA') AS fk_provider_ordering_id
	--, c.src_clm_pos_cd AS facility_place_of_service_cd    
    , bb_eob_item.src_location_code AS facility_place_of_service_cd
	, bb_eob_item.src_revenue --map to facility_revenue_center_cd 
	, '#NA' AS fk_provider_dispensing_id
    , '#NA' AS provider_dispensing_id_type_cd
    , '#NA' AS fk_provider_prescribing_id
    , '#NA' AS provider_prescribing_id_type_cd	
	, TO_ARRAY('#NA') AS fk_diagnosis_id_list
    , TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_9_cd_list
    , TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_10_cd_list
	, '#NA' AS fk_procedure_id
    , '#NA' AS procedure_betos_cd
    , '#NA' AS procedure_hcpcs_cd
	, TO_ARRAY('#NA') AS procedure_hcpcs_mod_cd_list
    , '#NA' AS procedure_icd_9_cd
    , '#NA' AS procedure_icd_10_cd
    , '#NA' AS service_op_type_cd
	, bb_eob_item.src_category AS service_cms_type_cd   
	, 0.0 AS service_units
	
	, bb_eob_item.src_service
	, bb_eob.SRC_PROVIDER_REFERENCE 
	, bb_eob.SRC_FACILITY_REFERENCE 
	, bb_eob.SRC_ORGANIZATION_REFERENCE 
	, bb_eob_item.src_location_reference
	, bb_eob.src_patient_reference
FROM DEV_HUMANA.ods.bb_eob
JOIN DEV_HUMANA.ods.bb_eob_item
	ON bb_eob.pk_eob_id = bb_eob_item.fk_eob_id
LEFT JOIN allPrvdrNPILabel
	ON bb_eob.pk_eob_id = allPrvdrNPILabel.fk_eob_id
LEFT JOIN primaryOnlyPrvdrNPILabel
	ON bb_eob.pk_eob_id = primaryOnlyPrvdrNPILabel.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type in ('81'
							,'82'
	)

	
--med filtering
--826,814

WITH eobid --also done for phys
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
	'https://bluebutton.cms.gov/resources/variables/pde_id'  --pde claims don't populate this?
	, 'https://bluebutton.cms.gov/resources/identifier/claim-group'
	, 'https://bluebutton.cms.gov/resources/variables/rx_srvc_rfrnc_num'
	)
	) AS p (fk_eob_id, pde_id, claim_group, rx_srvc_rfrnc_num)	
)
, allProvdrData
AS 
(
SELECT fk_eob_id
	, src_responsible
	, src_role
	, CASE 
		WHEN src_role = 'primary' THEN 1
		WHEN src_role = 'supervisor' THEN 2
		WHEN src_role = 'assist' THEN 3
		WHEN src_role = 'other' THEN 4
		ELSE 5
	  END role_value
	, src_qualification
	, src_sequence
	, src_provider_npi
	, ROW_NUMBER() OVER (PARTITION BY fk_eob_id ORDER BY src_sequence) AS rwNbr
FROM ods.BB_EOB_CARE_TEAM
WHERE record_status_cd = 'a'
ORDER BY fk_eob_id
	, src_sequence
)
--array agg sorting w/in groups identifying role vs. ID type (NPI) or activity_type_cd
, allPrvdrRoleLabel
AS 
(
select fk_eob_id
	, array_agg(src_role || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty
FROM allProvdrData
GROUP BY fk_eob_id
)
, allPrvdrNPILabel
AS 
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty	
FROM allProvdrData
GROUP BY fk_eob_id
)
, primaryOnlyPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty		
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('primary')) = TRUE   
GROUP BY fk_eob_id
)
, allOtherPrvdrNPILabel
AS
(
select fk_eob_id
	, array_agg('npi_num' || '|' || src_provider_npi) within group(order by src_sequence) as providerID
	, array_agg(src_role) within group(order by src_sequence) as providerRole
	, array_agg(src_qualification) within group(order by src_sequence) as providerSpclty			
FROM allProvdrData
WHERE array_contains(src_role::variant, array_construct('supervisor','assist','other')) = TRUE   
GROUP BY fk_eob_id
)
, bb_eob_info
AS 
(
	SELECT fk_eob_id
		, record_status_cd 
		, src_sequence 
		, src_category
		, src_code 
		, src_value_string
	FROM ods.BB_EOB_INFORMATION 
	WHERE record_status_cd = 'a'
	ORDER BY FK_EOB_ID 
		, SRC_SEQUENCE 
)

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
--	, 'med'||'|'||bb_eob_care_team.src_provider_npi||'|'||bb_eob.src_patient_reference||'|'||
--	  to_char(COALESCE(bb_eob_item.src_serviced_date,CAST('1900-01-01' AS DATE)),'YYYYMMDD') AS fk_visit_id
	, 'med'||'|'||split_part(primaryOnlyPrvdrNPILabel.providerID[0],'|',2)||'|'||bb_eob.src_patient_reference||'|'||
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
	, allPrvdrNPILabel.providerID AS fk_provider_id_list
	, trim(primaryOnlyPrvdrNPILabel.providerID[0]) AS fk_provider_primary_id
	, '#NA' AS fk_provider_operating_id
    , '#NA' AS fk_provider_attending_id
    , '#NA' AS fk_provider_other_id
    , '#NA' AS fk_provider_rendering_id
    , '#NA' AS provider_rendering_specialty_cd
    , '#NA' AS provider_rendering_type_cd
    , '#NA' AS fk_provider_pay_to_id
    , '#NA' AS fk_provider_ordering_id	
    , '#NA' AS fk_provider_dispensing_id
	, '#NA' AS provider_dispensing_id_type_cd
	, '#NA' AS fk_provider_prescribing_id
	, '#NA' AS provider_prescribing_id_type_cd
 	, TO_ARRAY('#NA') AS fk_diagnosis_id_list  
 	, TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_9_cd_list
	, TO_ARRAY('#NA') AS diagnosis_provider_detail_icd_10_cd_list 	
	, '#NA' AS fk_procedure_id 	
	, '#NA' AS procedure_betos_cd 	
	, '#NA' AS procedure_hcpcs_cd 
	, TO_ARRAY('#NA') AS procedure_hcpcs_mod_cd_list
    , '#NA' AS procedure_icd_9_cd
    , '#NA' AS procedure_icd_10_cd
    , '#NA' AS service_op_type_cd
    , '#NA' AS service_cms_type_cd
    , 0.0 AS service_units
	--'ndc_spl_cd'||'|'||COALESCE(c.src_clm_line_ndc_cd,'#NA')	
	, 'ndc_spl_cd'||'|'||COALESCE(bb_eob_item.src_service,'#NA') AS fk_medication_id
 	, COALESCE(bb_eob_item.src_service,'#NA') AS medication_ndc_spl_cd
 	, '#NA' AS medication_hcpcs_cd	
 	, COALESCE(bb_eob_info.src_code,'#NA') AS medication_dispensing_status_cd
	, COALESCE(bb_eob_info2.src_code,'#NA') AS medication_dispense_as_written_code
	, COALESCE(eobidPvt.rx_srvc_rfrnc_num,'#NA') AS medication_dispense_ref_num	

	, bb_eob_item.src_quantity
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
--LEFT JOIN ods.BB_EOB_CARE_TEAM   --src_provider_npi
--	ON bb_eob.pk_eob_id = bb_eob_care_team.fk_eob_id
--	AND bb_eob_care_team.record_status_cd = 'a'
--	AND bb_eob_care_team.SRC_SEQUENCE = 2
LEFT JOIN primaryOnlyPrvdrNPILabel
	ON bb_eob.pk_eob_id = primaryOnlyPrvdrNPILabel.fk_eob_id
LEFT JOIN allPrvdrNPILabel
	ON bb_eob.pk_eob_id = allPrvdrNPILabel.fk_eob_id
LEFT JOIN bb_eob_info 
	ON bb_eob.pk_eob_id = bb_eob_info.fk_eob_id
		AND bb_eob_info.src_category = 'https://bluebutton.cms.gov/resources/variables/dspnsng_stus_cd'
LEFT JOIN bb_eob_info AS bb_eob_info2
	ON bb_eob.pk_eob_id = bb_eob_info2.fk_eob_id
		AND bb_eob_info2.src_category = 'https://bluebutton.cms.gov/resources/variables/daw_prod_slctn_cd'
LEFT JOIN eobidPvt
	ON bb_eob.pk_eob_id = eobidPvt.fk_eob_id
WHERE bb_eob.RECORD_STATUS_CD = 'a'
	AND bb_eob_item.record_status_cd = 'a'
	--AND load_period = 'm-2021-03'
	AND SUBSTRING(bb_eob.LOAD_PERIOD, 3,7) = '2021-03'
	AND bb_eob.src_type = 'PDE'
