-- drop MATERIALIZED VIEW dwd_customer_receveiable_detail_d_view;

-- REFRESH MATERIALIZED VIEW dwd_customer_receveiable_detail_d_view;

-- CREATE MATERIALIZED VIEW dwd_customer_receveiable_detail_d_view2
-- DISTRIBUTED BY HASH(`project_id`) BUCKETS 12
-- PARTITION BY  date_trunc('month', NOW())
--  as
 insert overwrite  dwd_customer_receveiable_detail_d -- partition(p202305)

 select 
        DATE(tt1.receivable_month) month,
        tt1.tenant_id,
        project_id,
        customer_id,
		customer_code,
        charge_object_id,
		charge_object_code,
        tt1.product_id,
        business_code,
		DATE(tt1.day)  as day,
		max(product_name) product_name,
        max(customer_name) customer_name,
		max(charge_object_name) charge_object_name,
        customer_phone,
        acct_no,
        customer_source_id,
        
        max(tt1.lev1_format_name)  lev1_format_name,
        max(tt1.lev2_format_name)  lev2_format_name,
        category_id,
       max(tt1.category_name) category_name,
      
       max(project_name) project_name,
       tt1.building_id,
       max(building_name) building_name,
       build_area,
      
       room_no,
       product_price,
       contract_id,
       contract_item_area,
       contract_item_price,
       start_date,
       end_date,
       build_area_total,
       sum(case when order_type='GENERAL' then receivable_amount else 0 end) as receivable_amount,
       sum(case when order_type='GENERAL' then arrears_amount else 0 end) as arrears_amount,
       sum(case when order_type='PENAL' then receivable_amount  else 0 end) as rec_penal_amount,
	   sum(case when order_type='PENAL' then actual_pay_amount  else 0 end) as actual_pay_amount
 from(
 select         
                t.day,
                t.receivable_month,
                t.receivable_id,
                t.tenant_id,
                t.project_id,            
                t.customer_id,
				t.customer_code,
                max(t2.name) customer_name, 
				max(t.charge_object_name) charge_object_name,
                t.customer_phone,
                ifnull(t.acct_no,'') as acct_no ,
                t.customer_source_id,
                t.charge_object_id,
                t.charge_object_type,
				t.charge_object_code,
                max(t.lev1_format_name) lev1_format_name,
                max(t.lev2_format_name) lev2_format_name,
                t.category_id, 
                max(t.category_name) category_name,
                t.product_id,
				max(t.product_name) product_name,
                max(t.project_name) project_name,
                t.building_id,
                max(t.building_name) building_name,
                t.room_no,
                t.build_area,
                t.business_code,                  
                t.category_code,
                case when t.order_type='ADJUST' then 'GENERAL' else t.order_type end as order_type,       
                min(t.product_price) as product_price,             
                sum(case when  t.order_line_state!='CANCELLED' and t.order_type in ('GENERAL','ADJUST') then t.total_amount else 0 end) as receivable_amount, --本月应收
				0 actual_pay_amount,
                sum(case when  t.order_line_state!='CANCELLED' and t.order_type in ('GENERAL','ADJUST') then t.arrears_amount else 0 end ) as arrears_amount

           from 
         	data_warehouse_rt.dwd_sf_customer_product_receivable_d 	t	
           left join data_warehouse_rt.dim_core_org_project t3
             on t.project_id=t3.id
            and t.tenant_id=t3.tenant_id
		   left join data_warehouse_rt.dim_core_psn_customer t2 on t.customer_id=t2.source_id and t.user_type=t2.type 
          where (t.business_code in (100101,100201) or (t.business_code in(200101,200201) and t.order_line_state='PAYED'))
            and t.order_line_state!='CANCELLED'
            and order_type not  in ('PENAL')
		-- 	and order_line_id=${order_line_id}
           -- and receivable_month = date_trunc('month', CURRENT_DATE())
         --   and t.charge_object_code='PLANHOUSE|109241'   and t.product_id='1641'
          group by          		 
          		  t.day,
                  t.receivable_month, 
                   t.receivable_id,
                   t.tenant_id,
                   t.project_id,
                   t.building_id,
                   t.customer_id,
				   t.customer_code,
                   t.customer_source_id,
                   t.charge_object_id,
                   t.charge_object_type,
				   t.charge_object_code,
                   t.product_id,
                   t.customer_phone,
                   t.category_id,
                   t.category_code,
                   t.business_code,
                   (case when t.order_type='ADJUST' then 'GENERAL' else t.order_type end),
                   t.room_no,
                   t.build_area,
                   ifnull(t.acct_no,'')

                   
              union ALL 
              
                     select                   
		                substr(cast(t1.update_time as string),1,10) as   day,
		                t.settle_month,
		                t1.receivable_id,
		                t1.tenant_id,
		                t1.project_id,            
		                t1.customer_id,
						t1.customer_code,
		                max(t3.name) customer_name, 
		                max(t1.charge_object_name) charge_object_name,
		                t1.customer_phone,
		                ifnull(t1.acct_no,'') as acct_no ,
		                t1.customer_source_id,
		                t1.charge_object_id,
		                t1.charge_object_type,
						t1.charge_object_code,
		                max(t1.lev1_format_name) lev1_format_name,
		                max(t1.lev2_format_name) lev2_format_name,
		                t1.category_id, 
		                max(t1.category_name) category_name,
		                t1.product_id,
		                max(t1.product_name) product_name,
		                max(t1.project_name) project_name,
		                t2.building_id,
		                max(t1.building_name) building_name,
		                t1.room_no,
		                t1.build_area,
		                t1.business_code,                  
		                t1.category_code,
		               'PENAL' AS  order_type,         
		                min(t1.product_price) as product_price,       
                      sum(case when  ifnull(t.initial_flag,'')='REDHEDGE' then 0 else settle_amount+deduct_amount end) as receivable_amount,  
                      sum(actual_pay_amount) actual_pay_amount,					  
                      0  AS  arrears_amount
                 from data_warehouse_rt.dwd_charge_rec_penal_settle t
                 left join data_warehouse_rt.dwd_sf_customer_product_receivable_d t1 on t1.receivable_id=t.receivable_id
                 left join data_warehouse_rt.dim_core_space_view t2 on t.charge_object_id=t2.source_id
                 left join data_warehouse_rt.dim_core_psn_customer t3 on t.customer_id=t3.source_id  and t1.user_type=t3.type 
                 and t.charge_object_type=t2.type
				 where   t1.receivable_id is not null -- and t1.delete_flag=0
				 --    and order_line_id=${order_line_id}
			     -- and settle_month=  date_trunc('month', CURRENT_DATE())
                group by 
		                substr(cast(t1.update_time as string),1,10) ,
		                t.settle_month,
		                t1.receivable_id,
		                t1.tenant_id,
		                t1.project_id,            
		                t1.customer_id,
						t1.customer_code,
		                t1.customer_phone,
		                ifnull(t1.acct_no,'') ,
		                t1.customer_source_id,
		                t1.charge_object_id,
						t1.charge_object_code,
		                t1.charge_object_type,
		                t1.category_id, 
		                t1.product_id,
		                t2.building_id,
		                t1.room_no,
		                t1.build_area,
		                t1.business_code,                  
		                t1.category_code
)tt1
LEFT JOIN
     (
     SELECT contract_id,
             receivable_id,
             left(cast(t1.start_date AS string),10) AS start_date,
             left(cast(t1.end_date AS string),10) AS end_date ,
             max(t.contract_item_area) AS contract_item_area,
             max(t.contract_item_price) AS contract_item_price
      FROM dwd_charge_ctr_contract_preview_line t
      LEFT JOIN dwd_charge_ctr_contract t1 ON t.contract_id=t1.id
      WHERE receivable_id!=0
        AND t.delete_flag=0
      GROUP BY contract_id,
               receivable_id,
               left(cast(t1.start_date AS string),10),
               left(cast(t1.end_date AS string),10)
      ORDER BY contract_id
      )
      tt2
      ON tt1.receivable_id=tt2.receivable_id
left join (
             select      building_id ,
                         lev1_format_name,                  
                         lev2_format_name,
                         sum(build_area)  as build_area_total
                    from (
                          select building_id ,
                                 building_name,
                                 charge_object_id ,
                                 lev1_format_name,
                                 lev2_format_name,
                                 max(build_area) as build_area 
                             from dwd_sf_customer_product_receivable_d_view 
                            group by building_id,
                                     charge_object_id,
                                     lev1_format_name,
                                     lev2_format_name,
                                     building_name ) t  
                      group by building_id,
                               lev1_format_name,                  
                               lev2_format_name 
          ) tt3
       on tt1.building_id=tt3.building_id 
      and tt1.lev1_format_name=tt3.lev1_format_name
      and tt1.lev2_format_name=tt3.lev2_format_name
 group by  
        tt1.day,
        tt1.receivable_month,
        tt1.tenant_id,
        project_id,
        customer_id,
		customer_code,
		charge_object_id,
		charge_object_code,
        tt1.product_id,
        business_code,
        customer_phone,
        acct_no,
        customer_source_id,
        category_id,
       tt1.building_id,
       build_area,
       room_no,
       product_price,
       contract_id,
       contract_item_area,
       contract_item_price,
       start_date,
       end_date,
       build_area_total
  