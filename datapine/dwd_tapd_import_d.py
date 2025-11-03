# -*- coding: utf-8 -*-
import subprocess
import pandas as pd
import csv
import uuid
import re
import os
import openpyxl
import shutil
from datetime import datetime

destination_folder = '/opt/data/tapd_bak'

folder_path = '/opt/data/tapd'  # 替换为你的文件夹路径


def generate_unique_label(method="uuid"):
    if method == "uuid":
        # 生成UUID并移除连字符
        label = str(uuid.uuid4()).replace('-', '')
    elif method == "timestamp":
        # 使用时间戳生成标签
        label = datetime.now().strftime('%Y%m%d%H%M%S')
    else:
        raise ValueError("Unsupported method. Use 'uuid' or 'timestamp'.")

    # 确保标签长度在1到128个字符之间
    if len(label) > 128:
        label = label[:128]

    # 检查标签是否符合正则表达式
    if not re.match(r'^[-\w]{1,128}$', label):
        raise ValueError(
            "Generated label  does not match the required format.".format(label))

    return label[-3:]


# 定义一个函数来去除字符串中的所有空白字符，包括中间的空格和空行
def remove_whitespace(text):
    if isinstance(text, str):
        # 去除中间的空白字符，包括空格和换行符
        return re.sub(r'\s+', ' ', text).strip()
    return text

# 定义一个函数来将逗号替换为全角逗号


def replace_commas(text):
    if isinstance(text, str):
        return text.replace(',', '，')
    return text


def streamload(import_file):
    label = generate_unique_label(method="timestamp")
    print("label:", label)
    # 定义命令和参数
    command = [
        "curl", "--location-trusted", "-u", "root:bztd@@@2024", "-H", "Expect: 100-continue",
        "-H", "label:{}".format(label), "-H", "column_separator:,",
        "-T", import_file, "-XPUT",
        "http://119.147.71.31:8030/api/data_warehouse_rt/dwd_tapd_import_d/_stream_load"
    ]

    #print("command:\n", command)

#     # 执行命令
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    print("Return code:", result.returncode)

#  # 构建MySQL命令
#     command2 = [
#         'mysql',
#         '-h', '192.168.18.113',
#         '-u', 'root',
#         '-P', '9030',
#         '-ppoly2023',
#         '-D', 'data_warehouse_rt',
#         '-e', "delete from dwd_tapd_story_task where id in ('0','ID')"
#     ]
#     # 执行命令
#     result2 = subprocess.run(command2, capture_output=True, text=True)
#     print("result2:", result2.returncode)

#     # 输出返回值、标准输出和标准错误

#     print("Output:\n", result.stdout)
#     print("Error:\n", result.stderr)

import csv
import chardet
import glob

# 输入和输出文件路径
input_file = "dwd_tapd_import_d.xlsx"
output_file = "dwd_tapd_import_d1.csv"

folder_path="/opt/script/python/tapd/sh/import"

merge_file ='/opt/script/python/tapd/sh/import/combined_import.csv'

def merge_excel(folder_path,merge_file):
    # 定义列的顺序
    column_order = [
       "姓名", "职位", "对用户访谈次数", "个人分享次数", "竞争力构想报告", "原创预研报告数","第三方应用接入数", "新技术&新工具实际采用数"
    ]

    # 读取文件夹中所有的 Excel 文件

    excel_files = [f for f in os.listdir(folder_path) if  f.endswith('.xlsx')]
    print("excel_files:",excel_files)
  

    # 创建一个空的 DataFrame 来存储所有数据
    combined_df = pd.DataFrame()


    # 遍历每个 Excel 文件
    for file in excel_files:
        file_path = os.path.join(folder_path, file)

        df = pd.read_excel(file_path, engine='openpyxl')
        # 提取文件名（不带扩展名）
        file_name = os.path.splitext(file)[0]
      

        # 按照 '-' 分割文件名，并取分割后的第一部分
        #df['文件名'] = file_name.split('_')[0]

        date_str = file_name.split('_')[4]
        print(f"date_str: {date_str}")
        # # 查找最后一个下划线的位置
        # underscore_pos = file_name.rfind('_')

        # # 查找文件扩展名的位置
        # dot_pos = file_name.rfind('.')

        # # 提取日期部分
        # if underscore_pos != -1 and dot_pos != -1:
        #     date_str = file_name[underscore_pos + 1:dot_pos]
        #     print(f"提取出的日期部分: {date_str}")
        # else:
        #     print("无法找到日期部分")

        # 使用正则表达式提取日期部分
        # match = re.search(r"_([0-9]{6})\.xlsx$", file_name)

        # if match:
        #     date_str = match.group(1)
        #     print(f"提取出的日期部分: {date_str}")
        # else:
        #     print("未找到符合格式的日期部分")

        #     print("date_str",date_str)

        # 解析字符串为年月
        year_month = datetime.strptime(date_str, "%Y%m")

        # 转换为目标格式的字符串
        formatted_date = year_month.strftime("%Y-%m-01")

        print("formatted_date",formatted_date)

        df['月份'] =  formatted_date 

                  # 筛选实际存在的列
        valid_columns = [col for col in column_order if col in df.columns]
        
        # 按照指定顺序排列列，并在最后添加新的一列
        df = df[['月份']+valid_columns ]
        
        # 将数据添加到总的 DataFrame 中
        combined_df = pd.concat([combined_df, df], ignore_index=True)

      # 保存处理后的数据到新的Excel文件
        combined_df.to_csv(merge_file, index=False,header=False, sep=',', encoding='utf-8',
              quoting=csv.QUOTE_NONE, escapechar='\\')
          



def convert_file(input_file, output_file):
    # Check if the file is .xlsx or .csv
    file_extension = os.path.splitext(input_file)[1].lower()

    if file_extension == '.xlsx':
        # Handle .xlsx file directly with pandas
        print(f"Detected .xlsx file: {input_file}")
      # 查找以 'dwd_tapd_import_d' 开头的 Excel 文件
        file_pattern = "dwd_tapd_import_d*.xlsx"
        files = glob.glob(file_pattern)
        # 遍历所有符合条件的文件并读取数据
        for file in files:
            print(f"File name: {file}")
            df = pd.read_excel(input_file, engine='openpyxl')
            df.to_csv(output_file, encoding='utf-8', index=False)  # Save as UTF-8 CSV
            print(f"File converted to CSV with UTF-8 encoding: {output_file}")
            streamload(output_file)

    elif file_extension == '.csv':
        # Detect file encoding for CSV
        with open(input_file, "rb") as f:
            result = chardet.detect(f.read())

        file_encoding = result['encoding']
        print(f"Detected file encoding for CSV: {file_encoding}")

        # If encoding is not UTF-8, convert to UTF-8
        if file_encoding.lower() != 'utf-8':
            with open(input_file, mode='r', encoding=file_encoding) as infile, \
                 open(output_file, mode='w', encoding='utf-8', newline='') as outfile:

                # Create CSV reader and writer
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                # Write rows from input to output
                for row in reader:
                    writer.writerow(row)

            print(f"CSV file converted to UTF-8 and saved as {output_file}")
            streamload(output_file)
        else:
            # If file is already in UTF-8, no conversion needed
            print(f"CSV file is already in UTF-8 format.")
            streamload(input_file)

    else:
        print(f"Unsupported file format: {file_extension}")

def streamload(import_file):
    label = generate_unique_label(method="timestamp")
    print("label:", label)
    # 定义命令和参数
    command = [
        "curl", "--location-trusted", "-u", "root:bztd@@@2024", "-H", "Expect: 100-continue",
        "-H", "label:{}".format(label), "-H", "column_separator:,",
        "-T", import_file, "-XPUT",
        "http://119.147.71.31:8030/api/data_warehouse_rt/dwd_tapd_import_d/_stream_load"
    ]

    #print("command:\n", command)

#     # 执行命令
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    print("Return code:", result.returncode)



#convert_file(input_file, output_file)

merge_excel(folder_path,merge_file)

streamload(merge_file)


