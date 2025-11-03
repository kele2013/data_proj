import openai
import pandas as pd
import logging
from datetime import datetime

# Step 1: 配置日志记录器
logging.basicConfig(filename='chat_classification_qw.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 设置 OpenAI API 密钥
openai.api_key = ''  # 请使用实际的API密钥
openai.api_base = 'http://127.0.0.1:8001/v1'  # 自定义服务器地址

# 使用 read_csv 读取 .csv 文件
df = pd.read_csv('messages.csv', encoding='utf-8')

# 确保 'StrTime' 字段是 datetime 格式，便于过滤
df['StrTime'] = pd.to_datetime(df['StrTime'], errors='coerce')
df['Date'] = df['StrTime'].dt.date  # 提取日期部分

# 获取用户输入的日期
#input_date_str = input("请输入要分析的日期（格式：YYYY-MM-DD）：")
#input_date = datetime.strptime(input_date_str, "%Y-%m-%d").date()
input_date =''

input_nicknames = ['香郡31#业主群','香郡32栋业主群','香郡33#业主群','香郡35#业主群','香郡36栋业主群','香郡37#业主群','香郡38#业主群','香郡50栋业主群']
for input_nickname in input_nicknames:
    # 过滤出指定日期的聊天记录
    #daily_messages = df[(df['Date'] == input_date) & (df['NickName'] == input_nickname)]
    daily_messages = df[(df['NickName'] == input_nickname)]

    #print(f"在 {input_date}，分组 '{input_nickname}' 的聊天记录：\n")
    #print(daily_messages[['StrContent', 'StrTime', 'Sender']])  #

    if daily_messages.empty:
        print(f"没有找到 {input_date} 的聊天记录。")
    else:
        # 提取当天的聊天内容并转换为字典格式
        messages = daily_messages[['StrTime', 'NickName', 'Sender', 'StrContent']].to_dict('records')
        
        # 构建当日聊天记录汇总内容
        chat_history = "\n".join([str(msg.get('StrContent', '') or '') for msg in messages])
        
        # 构建针对指定日期的聊天记录的提示
        prompt = f"请将以下{input_date}的聊天记录按照话题分类，并为每个话题生成工单，工单应包含问题的描述、发起人和责任人和建议处理方式还有咨询类的以及商业活动,工单中加入情感,并输出聊天记录原文\n\n"
        
        for msg in messages:
            prompt += f"时间: {msg['StrTime']}, 发送人: {msg['NickName']} {msg['Sender']}，消息内容: {msg['StrContent']}\n"

        # 调用自建 OpenAI 服务器进行分类和总结
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # 选择你自建API支持的模型
            messages=[
                {"role": "system", "content": "你是一个可以帮助分类和生成工单的助手。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=500
        )
        
        classification_summary = response['choices'][0]['message']['content']
        
        # 打印和记录当日的分类和总结结果
        print(f"{input_nickname} 分类和总结结果:\n", classification_summary)
        logging.info(f"{input_date} 分类和总结结果:\n{classification_summary}")
