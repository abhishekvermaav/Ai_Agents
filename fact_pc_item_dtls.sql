-- DROP PROCEDURE im_dwh.sp_fill_pc_item(int4);

CREATE OR REPLACE PROCEDURE im_dwh.sp_fill_pc_item(yearmonthday int4)
	LANGUAGE plpgsql
AS $$
	
	
   declare 
        mydate date; 
        myyear int; mymon int; myday int;
        curr_table_start_time timestamp; curr_table_end_time timestamp;
BEGIN
    curr_table_start_time := current_timestamp at time zone 'Asia/Kolkata';
    
    mydate:=current_timestamp at time zone 'Asia/Kolkata'-1;
    myyear:=extract(year from mydate);
    mymon:=extract(month from mydate);
    myday:=extract(day from mydate);
	
   
    execute 'delete from im_dwh.fact_pc_item_dtls where pc_item_id in ( select pc_item_id from im_data_lake.pc_item_new_temp )';	
	execute 'delete from im_stage_dwh.pc_item_modified_yesterday ';
	
	execute 'insert into im_stage_dwh.pc_item_modified_yesterday
	select 	a.fk_pc_item_id from
	(	select	fk_pc_item_id
		from 	im_dwh.fact_pc_item_doc 
		where 	date(pc_doc_modified_date)=date(current_timestamp at time zone ''Asia/Kolkata''-1)
		UNION	
		select	fk_pc_item_id
		from 	im_dwh.fact_pc_item_attribute
		where 	date(pc_item_attribute_mod_date)=date(current_timestamp at time zone ''Asia/Kolkata''-1)
		UNION	
		select 	fk_pc_item_id from
		(	select 	fk_pc_item_id, iil_display_flag , 
					row_number() over (partition by fk_pc_item_id order by added_date desc)rn
			from 	im_data_lake.iil_display_flag_details_incremental
			where  date(added_date)=date(current_timestamp at time zone ''Asia/Kolkata''-1)
		)A where rn =1
		UNION
		select 	iil_ecom_pc_item_id AS fk_pc_item_id
		from 	im_data_lake.iil_ecom_item
		where 	date(iil_ecom_item_add_date)=date(current_timestamp at time zone ''Asia/Kolkata''-1)
		UNION
		select 	fk_pc_item_id
		from 	im_dwh.fact_pc_item_to_glcat_mcat pc_mcat 
		where 	date(map_date)=date(current_timestamp at time zone ''Asia/Kolkata''-1)
		UNION
		select	fk_pc_item_id
		from 	im_dwh.fact_pc_item_ext ext
		where 	date(actual_last_updated_date)=date(current_timestamp at time zone ''Asia/Kolkata''-1)
	)A where fk_pc_item_id not in (select distinct pc_item_id from im_data_lake.pc_item_new_temp)';
	
execute 'delete from im_dwh.fact_pc_item_dtls where pc_item_id in ( select fk_pc_item_id from im_stage_dwh.pc_item_modified_yesterday)';

execute	'insert into im_dwh.fact_pc_item_dtls
             select pc_item_id, pc_item_glusr_usr_id, pc_item_display_id, pc_item_parent_id, pc_item_date, pc_item_modifieddate, pc_item_status, pc_item_status_approval, pc_item_img_small_isdefault, pc_item_hotnew, pc_item_is_ecom, pc_item_desc_small_flag, pc_item_img_small_flag, is_desc_name_same_flag, is_name_available, is_price_available, pc_item_admin_status, pc_item_aggr_display, fk_glcat_mcat_id, item_mapping_isprime, map_date, mcat_id_position, pc_item_tomcat_mcat_priority, iil_display_flag, mod_id, inserted_by_emp_id, catalog_score, null as listing_price, null as is_avl_online, fk_hindi_pdn_language_id, fob_price_absurd_reasonid, pc_item_moq_unit_type, isq_count, is_video_available, is_pdf_available, cust_isq_count, pc_item_desc_small_length, pc_item_name, pc_item_doc_date, fk_pc_item_rejection_code, ecom_variant_id, ecom_item_ref_id, item_pdn_review_status, is_hindi_name_available, pc_item_img_original_wh, pc_item_desc_det_len, pc_item_img_original_flag, pc_item_fob_price, pc_item_img_original,pc_item_img_small,pc_item_modified_by_user_date from 
			(select  pc.pc_item_id , pc.pc_item_glusr_usr_id , pc.pc_item_display_id , pc.pc_item_parent_id , pc.pc_item_date ,
					pc.pc_item_modifieddate , pc.pc_item_status , pc.pc_item_status_approval ,
					pc.pc_item_img_small_isdefault , pc.pc_item_hotnew , pc.pc_item_is_ecom , 
					pc.pc_item_desc_small_flag , pc.pc_item_img_small_flag , pc.is_desc_name_same_flag , 
					(case when length(pc.pc_item_name)>0 then 1 else 0 end) is_name_available ,
					(case when length(pc.pc_item_fob_price)>0 then 1 else 0 end)is_price_available,
					pc.pc_item_admin_status , pc.pc_item_aggr_display , pc_mcat.fk_glcat_mcat_id ,
					pc_mcat.item_mapping_isprime, pc_mcat.map_date, pc_mcat.mcat_id_position, pc_mcat.pc_item_tomcat_mcat_priority, dspl.iil_display_flag,
					ext.mod_id, ext.inserted_by_emp_id, ext.catalog_score, null as listing_price, null as is_avl_online, ext.fk_hindi_pdn_language_id,
					ext.fob_price_absurd_reasonid, pc.pc_item_moq_unit_type, pc_attr.isq_count, doc.is_video_available, doc.is_pdf_available,
					pc_attr.cust_isq_count, pc.pc_item_desc_small_length, pc_item_name::character varying(250),doc.pc_item_doc_date,pc.fk_pc_item_rejection_code,ecom_variant_id,
					ecom_item_ref_id, ext.item_pdn_review_status,
					(CASE WHEN length(nullif(TRIM(pc_item_hindi_name),''''))>0 THEN 1 ELSE 0 END)is_hindi_name_available,
					pc.pc_item_img_original_wh, pc.pc_item_desc_det_len,
                   (CASE WHEN length(nullif(TRIM(pc_item_img_original),''''))>0 THEN 1 ELSE 0 END)pc_item_img_original_flag,pc.pc_item_fob_price, pc_item_img_original,pc_item_img_small,pc_item_modified_by_user_date,ROW_NUMBER() OVER (PARTITION BY pc_item_id,fk_glcat_mcat_id ORDER BY pc_item_modifieddate DESC NULLS LAST) rn
			from  im_stage_dwh.pc_item_modified_yesterday left join 
			im_data_lake.pc_item pc on fk_pc_item_id=pc_item_id
			left join im_dwh.fact_pc_item_to_glcat_mcat pc_mcat on pc_item_id=pc_mcat.fk_pc_item_id
			left join im_dwh.fact_pc_item_ext ext on pc_item_id=ext.fk_pc_item_id
			left join
			(	select fk_pc_item_id,
					max(case when lower(trim(coalesce(pc_item_doc_type, pc_item_doc_title))) = ''video'' then 1 else null end)is_video_available,
					max(case when (lower(pc_item_doc_path) like ''%imimg.com%'' and lower(pc_item_doc_path) like ''%.pdf'') then 1 else null end)is_pdf_available,
					max(pc_item_doc_date)pc_item_doc_date
					from im_dwh.fact_pc_item_doc
					group by fk_pc_item_id
			)doc on	pc_item_id=doc.fk_pc_item_id
			left join
			(	select	fk_pc_item_id, count(distinct case when fk_im_spec_master_id>0 then fk_im_spec_master_id end)isq_count,
						count(case when fk_im_spec_master_id<0 then fk_im_spec_master_id end)cust_isq_count
				from 	im_dwh.fact_pc_item_attribute
				group by	fk_pc_item_id
			)pc_attr on pc_item_id=pc_attr.fk_pc_item_id
			left join im_data_lake.iil_ecom_item ecom on pc.pc_item_id=ecom.iil_ecom_pc_item_id
			left join 
			( 	select 	fk_pc_item_id, iil_display_flag from
				(	select 	fk_pc_item_id, iil_display_flag , 
							row_number() over (partition by fk_pc_item_id order by added_date desc)rn
					from 	im_data_lake.iil_display_flag_details_incremental
				)A where rn =1
			)dspl on pc.pc_item_id=dspl.fk_pc_item_id)pc where rn=1';

	
	
	execute	'insert into im_dwh.fact_pc_item_dtls
			select pc_item_id, pc_item_glusr_usr_id, pc_item_display_id, pc_item_parent_id, pc_item_date, pc_item_modifieddate, pc_item_status, pc_item_status_approval, pc_item_img_small_isdefault, pc_item_hotnew, pc_item_is_ecom, pc_item_desc_small_flag, pc_item_img_small_flag, is_desc_name_same_flag, is_name_available, is_price_available, pc_item_admin_status, pc_item_aggr_display, fk_glcat_mcat_id, item_mapping_isprime, map_date, mcat_id_position, pc_item_tomcat_mcat_priority, iil_display_flag, mod_id, inserted_by_emp_id, catalog_score, null as listing_price, null as is_avl_online, fk_hindi_pdn_language_id, fob_price_absurd_reasonid, pc_item_moq_unit_type, isq_count, is_video_available, is_pdf_available, cust_isq_count, pc_item_desc_small_length, pc_item_name, pc_item_doc_date, fk_pc_item_rejection_code, ecom_variant_id, ecom_item_ref_id, item_pdn_review_status, is_hindi_name_available, pc_item_img_original_wh, pc_item_desc_det_len, pc_item_img_original_flag, pc_item_fob_price, pc_item_img_original,pc_item_img_small,pc_item_modified_by_user_date from (select  pc.pc_item_id , pc.pc_item_glusr_usr_id , pc.pc_item_display_id , pc.pc_item_parent_id , pc.pc_item_date ,
					pc.pc_item_modifieddate , pc.pc_item_status , pc.pc_item_status_approval ,
					pc.pc_item_img_small_isdefault , pc.pc_item_hotnew , pc.pc_item_is_ecom , 
					pc.pc_item_desc_small_flag , pc.pc_item_img_small_flag , pc.is_desc_name_same_flag , 
					(case when length(pc.pc_item_name)>0 then 1 else 0 end) is_name_available ,
					(case when length(pc.pc_item_fob_price)>0 then 1 else 0 end)is_price_available,
					pc.pc_item_admin_status , pc.pc_item_aggr_display , pc_mcat.fk_glcat_mcat_id ,
					pc_mcat.item_mapping_isprime, pc_mcat.map_date, pc_mcat.mcat_id_position, pc_mcat.pc_item_tomcat_mcat_priority, dspl.iil_display_flag,
					ext.mod_id, ext.inserted_by_emp_id, ext.catalog_score, null as listing_price, null as is_avl_online, ext.fk_hindi_pdn_language_id,
					ext.fob_price_absurd_reasonid, pc.pc_item_moq_unit_type, pc_attr.isq_count, doc.is_video_available, doc.is_pdf_available,
					pc_attr.cust_isq_count, pc.pc_item_desc_small_length, pc_item_name::character varying(250),doc.pc_item_doc_date,pc.fk_pc_item_rejection_code,ecom_variant_id,
					ecom_item_ref_id, ext.item_pdn_review_status,
					(CASE WHEN length(nullif(TRIM(pc_item_hindi_name),''''))>0 THEN 1 ELSE 0 END)is_hindi_name_available,
					pc.pc_item_img_original_wh, pc.pc_item_desc_det_len,
                   (CASE WHEN length(nullif(TRIM(pc_item_img_original),''''))>0 THEN 1 ELSE 0 END)pc_item_img_original_flag,pc.pc_item_fob_price, pc_item_img_original,pc_item_img_small,pc_item_modified_by_user_date,ROW_NUMBER() OVER (PARTITION BY pc_item_id,fk_glcat_mcat_id ORDER BY pc_item_modifieddate DESC NULLS LAST)rn
			from    im_data_lake.pc_item_new_temp pc
			left join im_dwh.fact_pc_item_to_glcat_mcat pc_mcat on pc_item_id=pc_mcat.fk_pc_item_id
			left join im_dwh.fact_pc_item_ext ext on pc_item_id=ext.fk_pc_item_id
			left join
			(	select fk_pc_item_id,
					max(case when lower(trim(coalesce(pc_item_doc_type, pc_item_doc_title))) = ''video'' then 1 else null end)is_video_available,
					max(case when (lower(pc_item_doc_path) like ''%imimg.com%'' and lower(pc_item_doc_path) like ''%.pdf'') then 1 else null end)is_pdf_available,
					max(pc_item_doc_date)pc_item_doc_date
					from im_dwh.fact_pc_item_doc
					group by fk_pc_item_id
			)doc on	pc_item_id=doc.fk_pc_item_id
			left join
			(	select	fk_pc_item_id, count(distinct case when fk_im_spec_master_id>0 then fk_im_spec_master_id end)isq_count,
						count(case when fk_im_spec_master_id<0 then fk_im_spec_master_id end)cust_isq_count
				from 	im_dwh.fact_pc_item_attribute
				group by	fk_pc_item_id
			)pc_attr on pc_item_id=pc_attr.fk_pc_item_id
			left join im_data_lake.iil_ecom_item ecom on pc.pc_item_id=ecom.iil_ecom_pc_item_id
			left join 
			( 	select 	fk_pc_item_id, iil_display_flag from
				(	select 	fk_pc_item_id, iil_display_flag , 
							row_number() over (partition by fk_pc_item_id order by added_date desc)rn
					from 	im_data_lake.iil_display_flag_details_incremental
				)A where rn =1
			)dspl on pc.pc_item_id=dspl.fk_pc_item_id)pc where rn=1';
	
	execute	'delete from im_dwh.fact_pc_item_dtls
			where pc_item_id in
			(	select	pc_item_id from im_data_lake.pc_item_delete_archive
				where 	deleted_date_year='||myyear||' and deleted_date_month='||mymon||' and deleted_date_day>='||myday||')';
	execute	'update im_dwh.fact_pc_item_dtls
			set catalog_score=a.catalog_score
			from
			(
				select	fk_pc_item_id,catalog_score 			
				from im_dwh.fact_pc_item_ext
			)a
			where fact_pc_item_dtls.pc_item_id=a.fk_pc_item_id';
	
	commit;
	
    curr_table_end_time := current_timestamp at time zone 'Asia/Kolkata';	
    insert into admin.dwh_refresh_status
    values('im_dwh.fact_pc_item_dtls', '', curr_table_start_time, curr_table_end_time, true);
    
    commit;

END;


$$
;

-- Permissions

GRANT ALL ON PROCEDURE im_dwh.sp_fill_pc_item(int4) TO rd_scripusr;
GRANT ALL ON PROCEDURE im_dwh.sp_fill_pc_item(int4) TO biredshiftdb;
GRANT ALL ON PROCEDURE im_dwh.sp_fill_pc_item(int4) TO rd_shivam_usr;