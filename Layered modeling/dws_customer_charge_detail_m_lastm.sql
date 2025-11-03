-- INSERT OVERWRITE dws_customer_charge_detail_m partition(${now():format('yyyyMM'):prepend('p')})
INSERT OVERWRITE dws_customer_charge_detail_m partition(p202305)
 (month,tenant_id,project_id,customer_id,customer_code,charge_object_id,charge_object_code,product_id,product_name,business_code,customer_name,customer_phone,
 acct_no,lev1_format_name,lev2_format_name,category_id,category_name,product_price,project_name,building_id,building_name,build_area,build_area_total,room_no,
 receivable_amount,last_receivable_amount,year_receivable_amount,last_year_receivable_amount,receivable_penal_amount,last_receivable_penal_amount,cur_arrears_amount,
 last_arrears_amount,cur_year_arrears_amount,last_year_arrears_amount,actual_amount,actual_cur_amount,actual_last_amount,actual_cur_year_amount,actual_last_year_amount,actual_penal_amount,last_penal_amount,last_year_penal_amount,last_total_penal_amount,year_receivable_penal_amount,last_year_receivable_penal_amount,total_year_amount,
total_last_year_amount,total_last_amount,cur_advance_amount,last_advance_amount,total_deduct_amount,cur_deduct_amount,cur_advance_deduct_amount,last_left_amount,cur_refund_amount)

select  DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10))  as  month,   -- select CURRENT_DATE 
        tenant_id,
        project_id,
        customer_id,
        customer_code,
        charge_object_id,
        charge_object_code,
        product_id,
        product_name,
        business_code,  
        max(customer_name) customer_name,
        max(customer_phone) customer_phone,
        acct_no,       
        lev1_format_name,
        lev2_format_name,
        category_id,
        category_name,
      
        product_price,
        project_name,
        building_id,
       building_name,
       build_area,
	   max(build_area_total) build_area_total,     
       room_no,
       sum(receivable_amount) receivable_amount,
       sum(last_arrears_amount)+sum(actual_last_amount) last_receivable_amount, -- 当前往月应收
       sum(year_receivable_amount) year_receivable_amount,
       sum(last_year_receivable_amount) last_year_receivable_amount,
       sum(receivable_penal_amount) receivable_penal_amount,
       sum(last_receivable_penal_amount)+sum(last_penal_amount) last_receivable_penal_amount,
       sum(receivable_amount)-sum(actual_cur_amount) cur_arrears_amount, -- 本月欠款
       sum(last_arrears_amount) last_arrears_amount, -- 往月欠费
       sum(year_receivable_amount)-sum(total_year_amount) cur_year_arrears_amount, -- 本年欠费
       sum(last_year_arrears_amount)-sum(actual_last_year_amount) last_year_arrears_amount,--  往年欠费
       
       sum(actual_amount) actual_amount,
	   sum(actual_cur_amount) actual_cur_amount,
       sum(actual_last_amount) actual_last_amount,
       sum(actual_cur_year_amount) actual_cur_year_amount,
       sum(actual_last_year_amount) actual_last_year_amount,
	   
       sum(actual_penal_amount) actual_penal_amount,
       sum(last_penal_amount) last_penal_amount,
	   sum(last_year_penal_amount) last_year_penal_amount,
       sum(last_total_penal_amount) last_total_penal_amount,
	   
	   sum(year_receivable_penal_amount) year_receivable_penal_amount,
       sum(last_year_receivable_penal_amount) last_year_receivable_penal_amount,
  
       sum( total_year_amount) total_year_amount,
       sum(total_last_year_amount) total_last_year_amount,
       sum(total_last_amount) total_last_amount,
       sum(cur_advance_amount) cur_advance_amount,
       sum(last_advance_amount) last_advance_amount,
       sum(total_deduct_amount) total_deduct_amount,
	   sum(cur_deduct_amount)   cur_deduct_amount,
       sum(cur_advance_deduct_amount) cur_advance_deduct_amount,	   
       sum(last_advance_amount)-sum(last_deduct_amount) last_left_amount,
       sum(cur_refund_amount) cur_refund_amount
from(
select    

        tenant_id,
        project_id,
        customer_id,
        customer_code,
        max(customer_name) customer_name,
        max(customer_phone) customer_phone,
        acct_no,
        charge_object_id,
        charge_object_code,
        max(lev1_format_name) lev1_format_name,
        max(lev2_format_name) lev2_format_name,
        category_id,
        max(category_name) category_name,
        product_id,
        max(product_name) product_name,
        product_price,
        max(project_name) project_name,
        building_id,
       max(building_name) building_name,
       build_area,
	   build_area_total,
       business_code,
       room_no,
      0 receivable_amount,
     -- sum(case when month=202301 and receivable_month<'2023-01-01' then arrears_amount else 0 end) last_arrears_amount,--当前往月累计往月欠款
      0 last_receivable_amount,--当前往月应收
      
      0  year_receivable_amount,
      
      0   last_year_receivable_amount,
      
      0  receivable_penal_amount,--本月应收违约金
      
      0  last_receivable_penal_amount,--往月应收违约金
	  
	  0  year_receivable_penal_amount,-- 本年应收违约金
	  
	  0   last_year_receivable_penal_amount, -- 往年应收违约金
      
            
      0  cur_arrears_amount, --本月欠款
      
      0  last_arrears_amount, --往月欠款
      
      0 cur_year_arrears_amount, --本年欠费
      0 last_year_arrears_amount, --往年欠费
      
      sum(case when month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) then  actual_amount else 0 end) actual_amount,--本月实收  
      sum(case when month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and  receivable_month= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then penal_amount else 0 end) actual_penal_amount,--本月实收违约金
      sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then penal_amount else 0 end) last_penal_amount,--本月实收往月违约金
	  
	  sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and left(receivable_month,4)< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4)  then penal_amount else 0 end) last_year_penal_amount,--本月实收往年违约金
	   
      sum(case when  month< DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then penal_amount else 0 end) last_total_penal_amount,--往月实收往月违约金
      
       sum(case when  month=  DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then actual_cur_amount else 0 end) actual_cur_amount,--本月实收
       
      sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then actual_cur_amount else 0 end) actual_last_amount,--本月实收往月
      
      -- sum(case when  month>= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then actual_cur_amount else 0 end) actual_last_amount,--本月之后实收往月
  
      sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and  replace(left(receivable_month,4),'-','')   = left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4)  then actual_cur_amount else 0 end) actual_cur_year_amount,--本月实收本年
      sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and replace(left(receivable_month,4),'-','')   < left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4)  then actual_cur_amount else 0 end) actual_last_year_amount,--本月实收往年
      
      sum(case when YEAR(month)=YEAR(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10))) and left(receivable_month,4)  =left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4) then actual_cur_amount else 0 end) total_year_amount,--实收本年累计
      sum(case when YEAR(month)=YEAR(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10))) and left(receivable_month,4) <left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4) then actual_cur_amount else 0 end) total_last_year_amount,--实收往年累计
      
       sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month=left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then amount else 0 end ) total_last_amount, --累计往月已收
      
      sum(case when   month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and  receivable_month=left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then (advance_amount) else 0 end ) cur_advance_amount, --本月预收
      sum(case when  month<left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then (advance_amount) else 0 end ) last_advance_amount, --往月预收
	  sum(deduct_amount) total_deduct_amount, --总抵扣
      sum(case when   month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10))  then (deduct_amount) else 0 end ) cur_deduct_amount, --本月抵扣
      sum(case when  month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and  receivable_month=left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then (current_month_real_charge) else 0 end ) cur_advance_deduct_amount, --本月预收抵扣本月
      
      sum(case when  month<left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then deduct_amount else 0 end ) last_deduct_amount, --往月抵扣
      
      sum(case when   month= DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)) and receivable_month=left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then (refund_amount) else 0 end ) cur_refund_amount --本月退款
      
     from  dwd_customer_payment_detail_d
	  where business_code in (100101,100201,200101,200201) 
	  --  and charge_object_code ='PLANHOUSE|117619' and product_id ='1737'
	 -- and project_id =680 and room_no ='A-601' 
     -- where project_id =129 and building_id=956  and charge_object_id=72056
     -- where	month=  DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10))
group by 
        tenant_id,
        project_id,
        customer_id,
        customer_code,
        acct_no,
        charge_object_id,
        charge_object_code,
        category_id,
        product_id,
        product_price,
        building_id,
       build_area,
	   build_area_total,
       business_code,
       room_no
       
   union ALL 
   select  

        tenant_id,
        project_id,
        customer_id,
        customer_code,
        max(customer_name) customer_name,
        max(customer_phone) customer_phone,
        acct_no,
        charge_object_id,
        charge_object_code,
        max(lev1_format_name) lev1_format_name,
        max(lev2_format_name) lev2_format_name,
        category_id,
        max(category_name) category_name,
        product_id,
        max(product_name) product_name,
        product_price,
        max(project_name) project_name,
        building_id,
       max(building_name) building_name,
       build_area,
	   build_area_total,
       business_code,
       room_no,
      sum(case when  month= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then receivable_amount else 0 end ) receivable_amount,
     -- sum(case when month=202301 and receivable_month<'2023-01-01' then arrears_amount else 0 end) last_arrears_amount,--当前往月累计往月欠款
      sum(case when  month< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then receivable_amount else 0 end) last_receivable_amount,--当前往月应收
      
      
       sum(case when  left(month,4)= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4) and month<= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then receivable_amount else 0 end ) year_receivable_amount, --本年应收
     -- sum(case when month=202301 and receivable_month<'2023-01-01' then arrears_amount else 0 end) last_arrears_amount,--当前往月累计往月欠款
      sum(case when  left(month,4)< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4)  then receivable_amount else 0 end) last_year_receivable_amount,--往年应收
      
      
      sum(case when  month= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then rec_penal_amount else 0 end)  receivable_penal_amount,--本月应收违约金
      
      sum(case when  month< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10)  then rec_penal_amount-actual_pay_amount else 0 end)  last_receivable_penal_amount,--往月应收违约金
	  
	  sum(case when  left(month,4)= left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4)  then rec_penal_amount else 0 end)  year_receivable_penal_amount,--本年应收违约金
      sum(case when  left(month,4)< left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4)  then rec_penal_amount else 0 end)  last_year_receivable_penal_amount, --往年应收违约金
     -- sum(case when receivable_month=left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then (receivable_amount-actual_cur_amount) else 0 end ) cur_arrears_amount, --本月欠款
      sum(case when month=left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then arrears_amount else 0 end ) cur_arrears_amount, --本月欠款
      
      sum(case when month<left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),10) then arrears_amount else 0 end ) last_arrears_amount, --往月欠款
      
      sum(case when left(month,4) =left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4) then arrears_amount else 0 end ) cur_year_arrears_amount, --本年欠费
      sum(case when left(month,4)  <left(DATE(left(DATE_SUB(date_trunc('month', CURRENT_DATE),INTERVAL 1 MONTH),10)),4) then arrears_amount else 0 end ) last_year_arrears_amount, --往年欠费
      0 actual_amount,      
      0 actual_penal_amount,
      0 last_penal_amount,
	  0 last_year_penal_amount,
      0 last_total_penal_amount,
	
	  
      0 actual_cur_amount,
      0 actual_last_amount,
      
      0 actual_cur_year_amount,
      0 actual_last_year_amount,
      0 total_year_amount,
      0 total_last_year_amount,
      0 total_last_amount,
      0 cur_advance_amount,
      0 last_advance_amount,
	  0 total_deduct_amount,
      0 cur_deduct_amount,
      0 cur_advance_deduct_amount,
      0 last_left_amount,
      0 cur_refund_amount    
-- select *
from 
( 
-- SELECT *
-- FROM(
select * ,row_number() over(partition by  `month`,project_id, product_id,customer_code,charge_object_code,business_code,order_type,receivable_amount order by day desc ) rank1
from  dwd_customer_receveiable_detail_d where order_type='GENERAL'
and (receivable_amount!=0  or arrears_amount!=0 )
and  day< date_trunc('month', CURRENT_DATE)
-- )t1 
--  where rank1=1 

UNION ALL

select * ,row_number() over(partition by  `month`,project_id, product_id, customer_code,charge_object_code,business_code,order_type,rec_penal_amount order by day desc ) rank1
from  dwd_customer_receveiable_detail_d where order_type='PENAL'
and  day< date_trunc('month', CURRENT_DATE)


)t
 where business_code in (100101,100201,200101,200201)  and rank1=1 
--  and    charge_object_code='PLANHOUSE|117619'   and product_id='1737'
     group by 
        tenant_id,
        project_id,
        customer_id,
        customer_code,
        acct_no,
        charge_object_id,
        charge_object_code,
        category_id,
        product_id,
        product_price,
        building_id,
       build_area,
	   build_area_total,
       business_code,
       room_no
  )t 
  group by        
        tenant_id,
        project_id,
        customer_id,
        customer_code,
        acct_no,
        charge_object_id,
        charge_object_code,
        lev1_format_name,
        lev2_format_name,
        category_id,
        category_name,
        product_id,
        product_name,
        product_price,
        project_name,
        building_id,
       building_name,
       build_area,
       business_code,
       room_no