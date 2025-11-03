 -- drop MATERIALIZED VIEW dwd_customer_payment_detail_d_view
 -- REFRESH MATERIALIZED VIEW dwd_customer_payment_detail_d_view;
-- select * from dwd_customer_payment_detail_d_view
 
-- CREATE MATERIALIZED VIEW if not exists dwd_customer_payment_detail_d_view
-- DISTRIBUTED BY HASH(`project_id`) BUCKETS 12
-- as
 insert overwrite dwd_customer_payment_detail_d  partition(${now():format('yyyyMM'):prepend('p')})
-- insert overwrite dwd_customer_payment_detail_d   partition(p202305)

 
select  
      
        DATE(tt1.day)  as day,
        tt1.month,    
        project_id,
		customer_code,
        charge_object_id,
		charge_object_code,
		business_code,
		tt1.receivable_month,
        tt1.serial_no,       
        tt1.tenant_id,

        tt1.product_id,
        customer_id,
         cast(left(tt1.receivable_month,4) as bigint)  year,
        customer_name,
		charge_object_name,
        customer_phone,
        acct_no,
        bank_code,
        customer_source_id,
        tt1.lev1_format_name,
        tt1.lev2_format_name,
        category_id,
	   category_code,
       category_name,
	   tt1.product_name,
       tt1.product_price,
       project_name,
       tt1.building_id,
       building_name,
       build_area,

       room_no,
	   contract_id,
	   contract_item_area,
	   contract_item_price,
	   start_date,
	   end_date,
       tt3.receipt_no,
         ifnull(left(cast(tt3.create_time as string),10),'')  as receipt_time,
         tt4.build_area_total,
		 pay_way,
         pay_channel,
         pay_channel_name,
         pay_channel_sub_name,
         payee_user,
		 tt1.create_by,
		 receivable_amount,
         tt1.amount,
         actual_amount,
         actual_cur_amount,
         deduct_amount,
         advance_amount,
         advance_amount2,
		 current_month_real_charge,
         red_amount,
         penal_amount,
         refund_amount
FROM 
(
select       
               t.month,
               substr(cast(t.pay_time as string),1,10) as   day,
			   case when substr(t.serial_no,1,2)='14' then concat(substr(cast(t.pay_time as string),1,8),'01') else t1.receivable_month end  as receivable_month,
			   max(t1.receivable_id) receivable_id,
               t.tenant_id,
               t.project_id,
               t1.customer_id,
			   t1.customer_code,
               max(t1.customer_name) customer_name,
			   max(charge_object_name) charge_object_name,
               t1.customer_phone,
               ifnull(t1.acct_no,'') as acct_no ,
               t.bank_code,
               t1.customer_source_id,
               t1.charge_object_id,
			   t1.charge_object_code,
               max(t1.lev1_format_name) lev1_format_name,
               max(t1.lev2_format_name) lev2_format_name,
               t1.category_id,
			   max(t1.category_code) category_code,
               max(t1.category_name)  category_name,
               t1.product_id,
			   max( t1.product_name) product_name,
               max(t1.project_name) project_name,
               t1.building_id,
               max(t1.building_name) building_name,
               t1.build_area,
               t1.room_no,
               t1.business_code,
               max(t1.total_amount) receivable_amount,
              -- serial_no,
			   (case when substr(t.source_serial_no,1,2)='14' then t.source_serial_no else t.serial_no end) as serial_no,
			   max(pay_way) pay_way,
               max(pay_channel) pay_channel,
               max(pay_channel_name) pay_channel_name,
               max(pay_channel_sub_name) pay_channel_sub_name,
               max(payee_user) payee_user,
			   max(t.create_by)  create_by,
               -- case when t1.order_type='ADJUST' then 'GENERAL' else t1.order_type end as order_type,
                 min(t1.product_price) as product_price,
				sum(t.amount)  amount,
                sum(case when t1.order_line_state!='CANCELLED' and  t1.order_type!='REFUND' and t.pay_way!='DEDUCT-ADVANCE' then t.amount else 0 end) as actual_amount,
                sum (case when t1.order_line_state!='CANCELLED' and  t1.order_type not in('PENAL','ADVANCE','REFUND')  then t.amount else 0 end) as actual_cur_amount,
				sum(case when t1.order_line_state!='CANCELLED' and t.pay_way='DEDUCT-ADVANCE' then  t.amount else 0 end) as deduct_amount,
				sum(case when t1.order_line_state!='CANCELLED' and substr(t.serial_no,1,2)!='14' then  ifnull(t2.amount,0) else 0 end) advance_amount2,   
				sum(case when t1.order_line_state!='CANCELLED' and t1.order_type='ADVANCE' and   substr(t.serial_no,1,2)!='14' then  t.amount else 0 end ) as advance_amount,
                sum(case when t1.order_line_state!='CANCELLED' then  ifnull(t2.current_month_real_charge,0) else 0 end ) as current_month_real_charge,				
				sum(case when t1.order_line_state!='CANCELLED' and t1.order_type='REDHEDGE' then  t.amount else 0 end ) as red_amount,
				sum(case when t1.order_line_state!='CANCELLED' and order_type='PENAL' then t.amount  else 0 end) as penal_amount,
				sum(case when t1.order_line_state!='CANCELLED' and order_type='REFUND' then t.amount  else 0 end) as refund_amount
 -- select  t.month,t1.day,DATE(t.pay_time), t1.receivable_month ,t1.business_code,t.pay_channel,*
from data_warehouse_rt.dwd_sf_customer_order_payment_d  t
         left join
         (
	         select *,row_number() over(partition by order_line_id,tenant_id order by day desc) rank1
	         from data_warehouse_rt.dwd_sf_customer_product_receivable_d 
	         where  (total_amount!=0 or pay_amount!=0 or arrears_amount!=0)
         ) t1
          on t.order_line_id =t1.order_line_id
         and t.tenant_id =t1.tenant_id
         and t1.rank1=1
     --    and DATE(t.pay_time)=DATE(t1.day)
         and t1.order_line_state!='CANCELLED'
         left join (select  distinct t1.order_line_id ,t1.tenant_id ,t1.product_id,t.current_month_real_charge,t.amount
                    from data_warehouse_rt.dwd_charge_chr_advance t
                      left join data_warehouse_rt.dwd_charge_chr_advance_detail t1 on  t.id=t1.advance_id
                      )t2
         on t.order_line_id =t2.order_line_id
         and t.tenant_id =t2.tenant_id
		 and t.amount = abs(t2.amount)
        where 
        t1.business_code in (100101,100201,200101,200201,100301,100401,100402,100403) 
          and t.pay_channel!='TEMP_TO_ADVANCE'  
	      and t.month = date_trunc('month', CURRENT_DATE())	      
        --  and t.serial_no='13230524587515824032'
	  	--  and charge_object_code='PLANHOUSE|100871'   and t1.product_id='1425'  
	  	--  and t.order_line_id='1643176989898596424'
   
          group by    
              t.month,
              substr(cast(t.pay_time as string),1,10),          
               case when substr(t.serial_no,1,2)='14' then concat(substr(cast(t.pay_time as string),1,8),'01') else t1.receivable_month end,
            --   t1.receivable_id,
               t.tenant_id,
               t.project_id,
               t1.customer_id,
			   t1.customer_code,
               t1.customer_phone,
               ifnull(t1.acct_no,'') ,
               t.bank_code,
               t1.customer_source_id,
               t1.charge_object_id,
			   t1.charge_object_code,
               t1.category_id,
               t1.product_id,
               t1.building_id,
               t1.build_area,
               t1.room_no ,
               t1.business_code,
                (case when substr(t.source_serial_no,1,2)='14' then t.source_serial_no else t.serial_no end)
   )tt1 
 LEFT JOIN
     (
     SELECT contract_id,
             receivable_id,
             left(cast(t1.start_date AS string),10) AS start_date,
             left(cast(t1.end_date AS string),10) AS end_date ,
             max(t.contract_item_area) AS contract_item_area,
             max(t.contract_item_price) AS contract_item_price
      FROM data_warehouse_rt.dwd_charge_ctr_contract_preview_line t
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
left join
(  
    SELECT   serial_no ,
             receivable_month,
             product_id,
             group_concat( receipt_no) receipt_no,
             group_concat( left(cast(create_time AS string),10)) create_time
   from ( select distinct serial_no,receivable_month,product_id,receipt_no,create_time
   FROM dwd_charge_chr_charge_receipt_detail 
        )t
     group by serial_no,receivable_month,product_id
)tt3
on tt1.serial_no=tt3.serial_no
and tt1.product_id=tt3.product_id
and tt1.receivable_month = tt3.receivable_month
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
                             from data_warehouse_rt.dwd_sf_customer_product_receivable_d_view 
                            group by building_id,
                                     charge_object_id,
                                     lev1_format_name,
                                     lev2_format_name,
                                     building_name ) t  
                      group by building_id,
                               lev1_format_name,                  
                               lev2_format_name 
          ) tt4
       on tt1.building_id=tt4.building_id 
      and tt1.lev1_format_name=tt4.lev1_format_name
      and tt1.lev2_format_name=tt4.lev2_format_name

