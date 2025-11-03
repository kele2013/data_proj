-- drop MATERIALIZED VIEW  dwd_sf_customer_order_payment_d_view;

-- REFRESH MATERIALIZED VIEW dwd_sf_customer_order_payment_d_view;
-- CREATE MATERIALIZED VIEW dwd_sf_customer_order_payment_d_view
-- 
-- DISTRIBUTED BY HASH(`project_id`) BUCKETS 12
-- 
-- as

insert overwrite dwd_sf_customer_order_payment_d

 select t.id,
          t.order_line_id ,
          cast(left(cast(t4.pay_time as string),4) as bigint) as year,
          DATE(date_add(DATE(t4.pay_time),interval -day(t4.pay_time)+1 day))  as month,
          t.tenant_id ,
          t4.project_id ,
          case when ifnull(t4.old_payment_no,'')!='' then t4.old_payment_no else t.serial_no end as serial_no, 
          ifnull(t4.old_receipt_no,'')  as receipt_no, 
          t4.pay_time as pay_time ,
          ifnull(left(cast(t4.old_receipt_time as string),10),'') as receipt_time,
          t4.pay_channel,
         case 
		      when t4.pay_way='DEDUCT-REFUND' then '抵扣'
			  when t4.pay_way='DEDUCT-ADVANCE' then '抵扣'
		      when t3.name is not null then t3.name 
		      when t5.name is not null then  t5.name -- split(t5.pay_way,'-')[1]

			  else  '其他'
			 end as pay_channel_name,
		case  when t4.pay_way='DEDUCT-REFUND' then '抵扣'
			  when t4.pay_way='DEDUCT-ADVANCE' then '预收抵扣'
			  when t5.name is not null then t5.name
		      when t3.name is not null then t3.name 		     
			  else  '其他'
			 end as pay_channel_sub_name,
          t4.account_no,
          t2.bank_code,
          t.amount,
		  ifnull(if(t4.payee_user='','其他',t4.payee_user),'其他') as payee_user,
          left(cast(t4.pay_time as string),10) as day,
          cast(now() as string) as update_time ,
		  t4.pay_way,
		  t4.source_serial_no,
		  t4.create_by
     from data_warehouse_rt.dwd_charge_oms_pay_serial_order_line t 
     left join data_warehouse_rt.dwd_charge_oms_pay_serial t4 on t.pay_serial_id=t4.id
     left join (select bank_account ,bank_code from (
                                                     select *,row_number() over(partition by bank_account order by create_time desc) as rn 
                                                       from data_warehouse_rt.dwd_charge_chr_collection_contract where bank_account!='' ) t 
                                                      where rn=1 )
													  t2  on t4.account_no=t2.bank_account 
    left join dim_charge_config_charge_pay_way t3 on split(t4.pay_way,'-')[1] = t3.code 
	left join 
	(
	select concat(pay_way,'-',code) as pay_way,tenant_id ,max(name) name
	  from dim_charge_config_charge_org_custom 
	  group by  concat(pay_way,'-',code),tenant_id
	)t5 on
	t4.pay_way = t5.pay_way and t4.tenant_id= t5.tenant_id
     where t4.is_valid=1 
      and  DATE(date_trunc("MONTH",left(t4.pay_time,10)))=date_trunc("MONTH",CURRENT_DATE()) 
     ; 