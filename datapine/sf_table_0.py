#! /usr/bin/env python
# -*- coding:utf-8 -*-
# ====#====#====#====
# __author__ = "payne"
# Time : 2022/1/14 16:20
# FileName:
# Version: 1.0.0
# Describe:
# ====#====#====#====
import os
# import datetime
import sys

from email_utils import email
from read_conf import Getconf


def all_tables():
    # '-table', help='表名'
    # '-hash_field', help='hash字段'
    # '-hash_num', help='hash分片个数'
    # '-term', help='sql条件', default=''   # "-term=\"where .....\""
    # '-is_drop', help='是否重建表,ture/false', default=False
    # '-inc', action='store', help='是否执行datax,True/false', default=True
    # '-is_create', action='store', help='是否建表,True/false', default=True
    # '-mysqldatabases', help='mysql库'
    # '-mysql_ip', help='mysql ip'
    # '-mysql_port', help='mysql端口'
    # '-mysql_user', help='mysql用户'
    # '-mysql_pd', help='mysql库密码'
    # '-impala_ip', help='impala ip'
    # '-impala_port', help='impala 端口'
    # '-mysqldatabases', help='impala库'
    # '-kudu_port', help='kudu端口'
    # '-json_dir', help='json文件目录'   # 绝对路径,以'/'结束
    # '-prefix', help='impala表前缀'     # 无则 ''
    # '-suffix', help='impala表后缀'     # 无则 ''
    # '-the_conf', help='配置文件'
    # -is_write 是否重写json default=True
    the_table = [
        ["-table=cal_fee_category", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_category_template", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_odd_product_relation", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_odd_product_rule", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_product", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_product_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_product_object", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_product_relation", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_product_relation_sub", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_product_template", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_fee_rule", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_share_auto_rate", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_share_rule", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_share_rule_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_share_rule_meter", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=cal_share_rule_obj", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=chr_advance", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_advance_account", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_advance_detail", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_advance_rule", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=chr_charge_check_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_invoice", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_invoice_item", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_month_close", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_month_close_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_receipt", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_receipt_detail", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_receipt_seal", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_refund", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_charge_serial_check", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_collection_contract", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_collection_contract_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=chr_collection_contract_product", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=config_bank_org", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_code", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_code_relation", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_month_close", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_month_close_project", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_org", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_org_custom", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_org_project", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_charge_pay_way", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=config_collection", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=config_collection_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=ctr_contract", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=ctr_contract_item", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=ctr_contract_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=ctr_contract_preview", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=ctr_contract_preview_line", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_category", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=oms_order", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_order_line", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_pay_request", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_pay_request_order", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_pay_request_penal", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_pay_serial", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_pay_serial_order", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_pay_serial_order_line", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=oms_product", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=oms_user_order", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=rec_meter_reading", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=rec_meter_reading_log", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=rec_notice_policy", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=rec_notice_policy_project_relation", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True", "-prefix=dim_charge_"],
        ["-table=rec_payment_notice", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=rec_payment_notice_detail", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"],
        ["-table=rec_penal", "-hash_field=id", "-hash_num=5", "-is_drop=False", "-inc=True", "-is_create=False","-is_write=True"]
        
    ]
    return the_table


if __name__ == '__main__':
    the_conf = sys.argv[1]
    num = dict()

    the_table = all_tables()
    for table in the_table:
        args = ''
        for arg in table:
            args = args + arg + ' '

        result = os.system('python /opt/script/python/datax_sr_test/sf.py %s -the_conf=%s' % (args, the_conf))
        times = 0 if table[0][7:] not in num else num[table[0][7:]]
        if result != 0:
            print(table[0][7:] + "表数据同步失败")
#        if result != 0 and times < 10:
#            print(table[0][7:] + "表数据同步失败")
#            the_table.append(table)
#            num[table[0][7:]] = times + 1
#        elif result != 0 and times >= 10:
#            print(table[0][7:] + "表数据同步重试10次后失败失败")
#        else:
#            print(table[0][7:] + "表数据同步成功,重跑" + str(times) + "次")

    print("各表的重跑次数:" + str(num))
    email.send_email("各表的重跑次数:" + str(num))

