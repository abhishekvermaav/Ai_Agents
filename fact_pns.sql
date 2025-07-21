CREATE OR REPLACE PROCEDURE im_dwh.sp_fill_fact_pns(yearmonthday int4)
	LANGUAGE plpgsql
AS $$
	
	
    declare 
		mydate_part int;
        curr_table_start_time timestamp; curr_table_end_time timestamp;
BEGIN
    curr_table_start_time := current_timestamp at time zone 'Asia/Kolkata';
    
    if yearmonthday>0 then
        mydate_part:= yearmonthday;
    else
        select max(to_char(date(coalesce(pns_call_record_mod_date,pns_call_record_date)),'yyyymmdd'))::int into mydate_part from im_dwh.fact_pns;
    end if;
	    
    execute	'insert into im_dwh.fact_pns
			select  pns_call_record_id , pns_call_fenq_id, pns_call_record_date,
					(select date_id from im_dwh.dim_date where date(full_date)=date(pns.pns_call_record_date)) pns_call_record_date_id,
					pns_call_all_records_pri, 
					(case
						when pns_call_status in(''36'', ''46'', ''48'', ''73'', ''96'') then ''Direct forward_answered''
						when pns_call_status in(''35'', ''45'', ''56'', ''70'', ''95'') then ''Direct forward_missed''
						when pns_call_status in(''34'', ''44'', ''54'', ''69'', ''94'') then ''Dropped''
						when pns_call_status in(''31'', ''41'', ''53'', ''68'', ''91'') then ''Fail_answered''
						when pns_call_status in(''32'', ''42'', ''80'', ''82'', ''92'') then ''Fail_missed''
						when pns_call_status in(''33'', ''43'', ''81'', ''83'', ''93'') then ''Fail_no_cc''
						when pns_call_status in(''47'', ''97'') then ''No_pin_answered''
						when pns_call_status in(''48'', ''98'') then ''No_pin_missed''
						when pns_call_status in(''30'', ''40'', ''52'', ''67'', ''90'') then ''Success''
						else pns_call_status
					end) as pns_call_status_derived, pns_call_fenq_status, pns_call_duration , pns_call_center_duration, pns_call_ring_duration, 
					pns_call_vendor_type, pns_call_patch_vendor_status, pns_call_patch_duration, pns_call_patch_crm_status,
					pns_call_patch_time, pns_call_record_started_at, pns_call_record_ended_at, pns_call_caller_glusr_id ,
					pns_call_caller_cntry_iso, gl.fk_gl_city_id pns_call_caller_city_id,
					pns_call_caller_circle ,pns_call_caller_operator, pns_call_receiver_glusr_id, pns_call_rcv_ring_duration, 
					pns_custtype_id , pns_custtype_weight, pns_call_read_status , pns_call_first_read_date, pns_call_record_mod_date ,
					(select date_id from im_dwh.dim_date where date(full_date)=date(pns.pns_call_record_mod_date)) pns_call_record_mod_date_id,
					pns_call_modrefname, pns_call_vendor_uniqueid , pns_call_patch_number_masked , pns_call_patch_number_hashed , 
					pns_call_caller_number_hashed , pns_call_virtual_number_hashed , pns_call_virtual_number_3_digits , 
					pns_call_records_dns_id_hashed, pns_call_cc_ring_duration, pns_call_disconnect_by_cc, pns_call_disconnect_by_vendor, 
					pns_call_records_service_id, pns_call_ref_prime_mcat, pns_call_ref_prime_subcat, pns_call_sndr_latitude, 
					pns_call_sndr_latlong_accuracy, pns_call_sndr_longitude, pns_call_sndr_offhr_flag,
					pns_call_standard_transfertime, pns_call_status, pns_call_api_response_code, pns_call_rev_lookup_source, pns_call_status_reason_code, pns_call_source_modid,
					pns_call_patch_cc_status, pns_call_cc_associate_name,live_lead_api_response, seller_vintage_days, pns_call_virtual_number,pns_call_recording_url, pns_call_mcat_src_identifier
			from    im_data_lake.pns_call_all_records_new pns
			left join im_dwh.dim_glusr_usr gl on (pns.pns_call_caller_glusr_id = gl.glusr_usr_id)
			where   pns_call_date_part > '||mydate_part || ' and pns_call_date_part < to_char(current_date,''yyyymmdd'')::int';
    
	commit;
	
    curr_table_end_time := current_timestamp at time zone 'Asia/Kolkata';	
    insert into admin.dwh_refresh_status
    values('im_dwh.fact_pns', '', curr_table_start_time, curr_table_end_time, true);
    
    commit;
	
END;


$$
;

-- Permissions

GRANT ALL ON PROCEDURE im_dwh.sp_fill_fact_pns(int4) TO rd_scripusr;
GRANT ALL ON PROCEDURE im_dwh.sp_fill_fact_pns(int4) TO rd_shivam_usr;