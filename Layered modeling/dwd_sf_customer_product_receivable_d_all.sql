-- drop MATERIALIZED VIEW dwd_sf_customer_product_receivable_d_view;
-- ALTER MATERIALIZED VIEW dwd_sf_customer_product_receivable_d_view SET INCREMENTAL BY TIME 5 s;
-- SET materialized_view_incremental_mode=ON;

-- REFRESH MATERIALIZED VIEW dwd_sf_customer_product_receivable_d_view;

-- CREATE MATERIALIZED VIEW dwd_sf_customer_product_receivable_d_view
-- DISTRIBUTED BY HASH(`project_id`) BUCKETS 12

-- as
INSERT overwrite dwd_sf_customer_product_receivable_d -- partition(p202305)

SELECT t.id AS order_line_id,
       cast(replace(substr(cast(t.create_time AS string),1,7),'-','') AS bigint) AS MONTH,
       DATE(date_trunc("MONTH",left(t.create_time,10))) as  dt,
       t.tenant_id,
       t.shop_id AS project_id,
       t.relation_order_id,
       t1.user_id AS customer_id,
       t1.user_id AS customer_source_id,
       t1.user_type,
       CASE
           WHEN t1.charge_object_type='CONTRACT' THEN t9.building_id
           ELSE ifnull(t2.building_id,0)
       END AS building_id,
       t3.category_id as category_id,
       t3.category_code as category_code,
       t.product_id,
       cast(split_part(t.charge_object_code,'|',2) as bigint) AS charge_object_id,
        split_part(t.charge_object_code,'|',1) as charge_object_type,
	   t1.charge_object_name,
       t7.id AS receivable_id,
       t4.name AS project_name,
       t1.user_name AS customer_name,
       t1.user_mobile AS customer_phone,
       CASE
           WHEN t1.charge_object_type='CONTRACT' THEN ifnull(t9.building_name,'')
           ELSE ifnull(t2.building_name,'')
       END AS building_name,
        ifnull(t3.category_name,'') AS category_name,
       ifnull(t3.name,'') AS product_name,
       t.receivable_month AS receivable_month,
       t.type AS order_type,
       t.business_code,
       t.state_c_f AS order_line_state,
       CASE
           WHEN t1.charge_object_type='CONTRACT' THEN t9.name
           ELSE t2.name
       END AS room_no,
       if(split_part(cast(t3.price as string), '.', 2) = '', cast(t3.price as string),concat(split_part(cast(t3.price as string), '.', 1),if(replace(rtrim(replace(split_part(cast(t3.price as string), '.', 2),'0',' ')),' ','0') = '','',concat('.',replace(rtrim(replace(split_part(cast(t3.price as string), '.', 2),'0',' ')),' ','0'))))) AS product_price,
       CASE
           WHEN t1.charge_object_type='CONTRACT' THEN t9.build_area
           ELSE t2.build_area
       END AS build_area,
       t5.bank_account AS acct_no,
       CASE
           WHEN t1.charge_object_type='CONTRACT' THEN ifnull(t9.level1_format_name,'')
           ELSE ifnull(t2.level1_format_name,'')
       END AS lev1_format_name,
       CASE
           WHEN t1.charge_object_type='CONTRACT' THEN ifnull(t9.level2_format_name,'')
           ELSE ifnull(t2.level2_format_name,'')
       END AS lev2_format_name,
       t.total_amount ,
       t.pay_amount,
       t.balance_amount  as arrears_amount,
       left(cast(ifnull(t.update_time ,t.create_time) as string),10) as day,
       case when t.charge_object_code like '%CONTRACT%' then concat('CONTRACT','|',t1.contract_no) else t.charge_object_code end charge_object_code,
       concat(t1.user_type,'|',cast(t1.user_id as string)) as customer_code,
       cast(now() AS string) AS update_time 
FROM (
select * from data_warehouse_rt.dwd_charge_oms_order_line
--  where   DATE(date_trunc("MONTH",left(update_time,10)))=date_trunc("MONTH",CURRENT_DATE()) 
        -- and 
  --  charge_object_code='PLANHOUSE|121874' and product_id='1760'
) t
LEFT JOIN data_warehouse_rt.dwd_charge_oms_user_order t1 ON t.order_id=t1.order_id
AND t.tenant_id=t1.tenant_id
LEFT JOIN data_warehouse_rt.dim_core_space_view t2 ON t1.charge_object_id=t2.source_id
AND t1.charge_object_type=t2.type
LEFT JOIN data_warehouse_rt.dim_core_org_project t4 ON t.shop_id =t4.id
LEFT JOIN data_warehouse_rt.dim_charge_product_view t3 ON t.product_id=t3.id
LEFT JOIN
  (SELECT *
   FROM
     ( SELECT *,
              row_number() over(partition BY project_id ,customer_id,charge_object_id
                                ORDER BY create_time DESC) AS rn
      FROM data_warehouse_rt.dwd_charge_chr_collection_contract) t
   WHERE rn=1) t5 ON t1.user_id=t5.customer_id
AND t1.charge_object_id=t5.charge_object_id
LEFT JOIN data_warehouse_rt.dwd_charge_rec_receivable t7 ON t.tenant_id=t7.tenant_id
AND t.id=t7.order_line_id
LEFT JOIN data_warehouse_rt.dwd_charge_ctr_contract_item t8 ON t1.charge_object_id=t8.contract_id
AND t.tenant_id=t8.tenant_id
AND t1.charge_object_type='CONTRACT'
LEFT JOIN data_warehouse_rt.dim_core_space_view t9 ON t8.charge_object_id=t9.source_id
AND t9.type=t8.charge_object_type
