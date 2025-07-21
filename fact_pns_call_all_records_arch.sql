CREATE OR REPLACE VIEW im_dwh.def_pns_call_all_records_arch as
select	pns_call_record_id, pns_call_record_date, pns_call_record_date_id, pns_call_status,
		pns_call_receiver_glusr_id, pns_call_caller_number_hashed, pns_call_caller_circle,
		pns_call_caller_operator,pns_call_patch_duration, pns_call_caller_glusr_id, pns_custtype_id,
		pns_call_caller_cntry_iso, pns_call_ring_duration, pns_call_cc_ring_duration, 
		pns_call_rcv_ring_duration, pns_call_sndr_offhr_flag, pns_call_vendor_type, 
		pns_call_modrefname, pns_call_ref_prime_mcat, pns_call_standard_transfertime, 
		dtmf_blacklist_status, c2c_latest_call_id, c2c_count, live_lead_api_response, 
		pns_call_status_reason_code, pns_call_vendor_uniqueid, pns_call_source_modid, 
		pns_call_patch_cc_status,pns_call_recording_url, pns_call_duration,pns_call_mcat_src_identifier
FROM 
(	SELECT 	A.*, DT.date_id pns_call_record_date_id 
	FROM 
	(	select	pns_call_record_id, pns_call_record_date, pns_call_status, pns_call_receiver_glusr_id,
				pns_call_caller_number_hashed, pns_call_caller_circle, pns_call_caller_operator,
				pns_call_patch_duration, pns_call_caller_glusr_id, pns_custtype_id, 
				pns_call_caller_cntry_iso, pns_call_ring_duration, pns_call_cc_ring_duration,
				pns_call_rcv_ring_duration, pns_call_sndr_offhr_flag, pns_call_vendor_type, 
				pns_call_modrefname, pns_call_ref_prime_mcat, pns_call_standard_transfertime, 
				dtmf_blacklist_status, c2c_latest_call_id, c2c_count, live_lead_api_response,
				pns_call_status_reason_code, pns_call_vendor_uniqueid, pns_call_source_modid, pns_call_patch_cc_status,
				pns_call_recording_url::varchar(250),pns_call_duration,pns_call_mcat_src_identifier	
		FROM  	im_data_lake.PNS_CALL_ALL_RECORDS_ARCH 
		WHERE 	PNS_CALL_DATE_PART > 
			(	SELECT 	max(to_date(FULL_DATE, 'yyyyMMdd')) FROM im_data_lake.DIM_DATE 
				WHERE 	DATE_ID = (select max(PNS_CALL_RECORD_DATE_ID) from im_dwh.pns_call_all_records_arch)
			)
	) A 
	LEFT JOIN im_data_lake.DIM_DATE DT on(date(PNS_CALL_RECORD_DATE) = date(DT.FULL_DATE)))T
with no schema binding;

-- Permissions

GRANT ALL ON TABLE im_dwh.def_pns_call_all_records_arch TO biredshiftdb;
GRANT ALL ON TABLE im_dwh.def_pns_call_all_records_arch TO rd_scripusr;
GRANT SELECT ON TABLE im_dwh.def_pns_call_all_records_arch TO rd_sameeksha_94551;
GRANT SELECT ON TABLE im_dwh.def_pns_call_all_records_arch TO rd_amit_94680;
GRANT SELECT ON TABLE im_dwh.def_pns_call_all_records_arch TO rd_gurkirat_94535;
GRANT SELECT ON TABLE im_dwh.def_pns_call_all_records_arch TO GROUP dwh_developer;
GRANT SELECT ON TABLE im_dwh.def_pns_call_all_records_arch TO rd_shubham_87576;
GRANT SELECT ON TABLE im_dwh.def_pns_call_all_records_arch TO rd_abhishek_94744;c2c_latest_call_id