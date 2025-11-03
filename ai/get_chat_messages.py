import csv
import sys
sys.path.append(r"F:\hkc\AI\chatAI\WeChatMsg-master\WeChatMsg-master")
from app.DataBase.package_msg import PackageMsg
import sqlite3



def get_chatroom_id_by_name(chatroom_name):
    """
    根据群聊名称查找对应的 chatroom_id.
    """
    # 初始化 PackageMsg 实例
    db_path = 'F:/hkc/AI/chatAI/WeChatMsg-master/WeChatMsg-master/app/DataBase/Msg/MicroMsg.db'
    
    # 假设有获取数据库连接的方法
    conn = sqlite3.connect(db_path)  # `db_path` 是数据库文件路径，需根据实际情况调整
    cursor = conn.cursor()

    # 查询包含所有群聊的表，例如 chatrooms 表
    query = "SELECT UserName FROM Contact WHERE NickName = ?"
    cursor.execute(query, (chatroom_name,))
    result = cursor.fetchone()
    print("result:",result)

    conn.close()
    return result[0] if result else None
    
    

def export_chat_to_csv():
    """
    根据 chatroom_id 提取聊天记录并导出到 CSV 文件.
    """
    p = PackageMsg()
    #messages = p.get_package_message_by_wxid(chatroom_id)  # 获取聊天记录，假设返回一个列表
    messages = p.get_package_message_all()
    
    # 定义CSV文件名
    csv_filename = f"all_chat_records.csv"
    
    # 打开文件并写入
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # 写入表头
        writer.writerow(["StrContent", "StrTime", "NickName","Sender"])
        
        # 假设 messages 是一个包含聊天记录的列表，每条记录是字典格式
        for message in messages:
            #print(f"message:",message)
            writer.writerow([message[7], message[8], message[10],message[11]])
    
    print(f"all Chat records for exported successfully to {csv_filename}.")

def export_all_chats():
    """
    根据群聊名称列表自动导出所有群聊的聊天记录.
    """
    export_chat_to_csv()
    # for chatroom_name in chatroom_names:
    #     chatroom_id = get_chatroom_id_by_name2(chatroom_name)
    #     print(f"chatroom_id:{chatroom_id}")
    #     if chatroom_id:
    #         export_chat_to_csv(chatroom_id, chatroom_name)
    #     else:
    #         print(f"No chatroom found with the name '{chatroom_name}'.")

if __name__ == "__main__":
    export_chat_to_csv()
