import os
import platform
import queue
import threading
import time

import pandas as pd
from logger import Logger
from sqlalchemy import create_engine
from watchdog.events import *
from watchdog.observers import Observer

"""
TODO
(1)增加文件名过滤规则 不满足跳过 continue (1-1) 文件名 (1-2) 文件内容
(2)怎么处理已经入到文件夹中的文件?
(3)任务分发的时候修改导出文件格式,导出CSV 不是txt
(4)整理字典


已经基本完成将分发数据转移到mysql的程序,程序包含一下功能:
(1)实时监控目标文件夹,将分发进来的文件记录文件路径,加入一个同步队列.
(2)采用多线程+生产者消费者模型,提高文件解析入库效率.
(3)使用pandas可以读取处理海量CSV文件数据,避免数据量过大导致的读取过程卡死或耗时过长问题.
(4)一旦数据完成入库操作,立即删除分发文件.
在本地完成,由于本程序要处理包括35张小时级表和一张天级别表,且涉及字段过多,还需要继续完善下面字典变量
# 字典 {分发接口编号:[表名,[字段]]}
table_dict = {"TM00000": ['cinma', ["ID", "name", "tel", "parce"]], #测试
              'TM00001': ['if_index', []],
              'TM00002':[],
               ''''''
              'TM00036': ['site_info', []]}

"""

logger = Logger()
yconnect = create_engine('mysql+pymysql://root:liu@2014@39.108.231.238:3306/stu?charset=utf8')
# yconnect = create_engine('mysql+pymysql://wyznhfx:N_2510las@10.191.19.111:8066/wyznhfxgjdb?charset=utf8')


# 监控文件夹
MONITIOR_FOLDER = '/Users/anus/data'
# MONITIOR_FOLDER = '/home/CySftp/data/result'


# 同步队列
queue = queue.Queue()
print("***开始执行监控解析入库任务***")

# 字典 分发接口编号:[表名,字段]
table_dict = {"TM00000": ['cinma', ["ID", "name", "tel", "parce"]], # 测试用表
			  'TM00001': ['if_index',
						  ["city", "def_vendorname", "def_cellname", "def_cellname_chinese", "region", "town", "grid",
						   "ttime", "thour", "cnt_period", "cnt_if", "band", "num_valid_rbs", "ave_if_nl", "max_if_nl",
						   "num_if_rbs", "num_if_rbs_band1", "num_if_rbs_band2", "num_if_rbs_band3", "num_if_rbs_band4",
						   "num_if_rbs_band5", "if_index", "if_level", "if_type", "phy_ulmeannl_prb0",
						   "phy_ulmeannl_prb1", "phy_ulmeannl_prb2", "phy_ulmeannl_prb3", "phy_ulmeannl_prb4",
						   "phy_ulmeannl_prb5", "phy_ulmeannl_prb6", "phy_ulmeannl_prb7", "phy_ulmeannl_prb8",
						   "phy_ulmeannl_prb9", "phy_ulmeannl_prb10", "phy_ulmeannl_prb11", "phy_ulmeannl_prb12",
						   "phy_ulmeannl_prb13", "phy_ulmeannl_prb14", "phy_ulmeannl_prb15", "phy_ulmeannl_prb16",
						   "phy_ulmeannl_prb17", "phy_ulmeannl_prb18", "phy_ulmeannl_prb19", "phy_ulmeannl_prb20",
						   "phy_ulmeannl_prb21", "phy_ulmeannl_prb22", "phy_ulmeannl_prb23", "phy_ulmeannl_prb24",
						   "phy_ulmeannl_prb25", "phy_ulmeannl_prb26", "phy_ulmeannl_prb27", "phy_ulmeannl_prb28",
						   "phy_ulmeannl_prb29", "phy_ulmeannl_prb30", "phy_ulmeannl_prb31", "phy_ulmeannl_prb32",
						   "phy_ulmeannl_prb33", "phy_ulmeannl_prb34", "phy_ulmeannl_prb35", "phy_ulmeannl_prb36",
						   "phy_ulmeannl_prb37", "phy_ulmeannl_prb38", "phy_ulmeannl_prb39", "phy_ulmeannl_prb40",
						   "phy_ulmeannl_prb41", "phy_ulmeannl_prb42", "phy_ulmeannl_prb43", "phy_ulmeannl_prb44",
						   "phy_ulmeannl_prb45", "phy_ulmeannl_prb46", "phy_ulmeannl_prb47", "phy_ulmeannl_prb48",
						   "phy_ulmeannl_prb49", "phy_ulmeannl_prb50", "phy_ulmeannl_prb51", "phy_ulmeannl_prb52",
						   "phy_ulmeannl_prb53", "phy_ulmeannl_prb54", "phy_ulmeannl_prb55", "phy_ulmeannl_prb56",
						   "phy_ulmeannl_prb57", "phy_ulmeannl_prb58", "phy_ulmeannl_prb59", "phy_ulmeannl_prb60",
						   "phy_ulmeannl_prb61", "phy_ulmeannl_prb62", "phy_ulmeannl_prb63", "phy_ulmeannl_prb64",
						   "phy_ulmeannl_prb65", "phy_ulmeannl_prb66", "phy_ulmeannl_prb67", "phy_ulmeannl_prb68",
						   "phy_ulmeannl_prb69", "phy_ulmeannl_prb70", "phy_ulmeannl_prb71", "phy_ulmeannl_prb72",
						   "phy_ulmeannl_prb73", "phy_ulmeannl_prb74", "phy_ulmeannl_prb75", "phy_ulmeannl_prb76",
						   "phy_ulmeannl_prb77", "phy_ulmeannl_prb78", "phy_ulmeannl_prb79", "phy_ulmeannl_prb80",
						   "phy_ulmeannl_prb81", "phy_ulmeannl_prb82", "phy_ulmeannl_prb83", "phy_ulmeannl_prb84",
						   "phy_ulmeannl_prb85", "phy_ulmeannl_prb86", "phy_ulmeannl_prb87", "phy_ulmeannl_prb88",
						   "phy_ulmeannl_prb89", "phy_ulmeannl_prb90", "phy_ulmeannl_prb91", "phy_ulmeannl_prb92",
						   "phy_ulmeannl_prb93", "phy_ulmeannl_prb94", "phy_ulmeannl_prb95", "phy_ulmeannl_prb96",
						   "phy_ulmeannl_prb97", "phy_ulmeannl_prb98", "phy_ulmeannl_prb99", "phy_ulmaxnl_prb0",
						   "phy_ulmaxnl_prb1", "phy_ulmaxnl_prb2", "phy_ulmaxnl_prb3", "phy_ulmaxnl_prb4",
						   "phy_ulmaxnl_prb5", "phy_ulmaxnl_prb6", "phy_ulmaxnl_prb7", "phy_ulmaxnl_prb8",
						   "phy_ulmaxnl_prb9", "phy_ulmaxnl_prb10", "phy_ulmaxnl_prb11", "phy_ulmaxnl_prb12",
						   "phy_ulmaxnl_prb13", "phy_ulmaxnl_prb14", "phy_ulmaxnl_prb15", "phy_ulmaxnl_prb16",
						   "phy_ulmaxnl_prb17", "phy_ulmaxnl_prb18", "phy_ulmaxnl_prb19", "phy_ulmaxnl_prb20",
						   "phy_ulmaxnl_prb21", "phy_ulmaxnl_prb22", "phy_ulmaxnl_prb23", "phy_ulmaxnl_prb24",
						   "phy_ulmaxnl_prb25", "phy_ulmaxnl_prb26", "phy_ulmaxnl_prb27", "phy_ulmaxnl_prb28",
						   "phy_ulmaxnl_prb29", "phy_ulmaxnl_prb30", "phy_ulmaxnl_prb31", "phy_ulmaxnl_prb32",
						   "phy_ulmaxnl_prb33", "phy_ulmaxnl_prb34", "phy_ulmaxnl_prb35", "phy_ulmaxnl_prb36",
						   "phy_ulmaxnl_prb37", "phy_ulmaxnl_prb38", "phy_ulmaxnl_prb39", "phy_ulmaxnl_prb40",
						   "phy_ulmaxnl_prb41", "phy_ulmaxnl_prb42", "phy_ulmaxnl_prb43", "phy_ulmaxnl_prb44",
						   "phy_ulmaxnl_prb45", "phy_ulmaxnl_prb46", "phy_ulmaxnl_prb47", "phy_ulmaxnl_prb48",
						   "phy_ulmaxnl_prb49", "phy_ulmaxnl_prb50", "phy_ulmaxnl_prb51", "phy_ulmaxnl_prb52",
						   "phy_ulmaxnl_prb53", "phy_ulmaxnl_prb54", "phy_ulmaxnl_prb55", "phy_ulmaxnl_prb56",
						   "phy_ulmaxnl_prb57", "phy_ulmaxnl_prb58", "phy_ulmaxnl_prb59", "phy_ulmaxnl_prb60",
						   "phy_ulmaxnl_prb61", "phy_ulmaxnl_prb62", "phy_ulmaxnl_prb63", "phy_ulmaxnl_prb64",
						   "phy_ulmaxnl_prb65", "phy_ulmaxnl_prb66", "phy_ulmaxnl_prb67", "phy_ulmaxnl_prb68",
						   "phy_ulmaxnl_prb69", "phy_ulmaxnl_prb70", "phy_ulmaxnl_prb71", "phy_ulmaxnl_prb72",
						   "phy_ulmaxnl_prb73", "phy_ulmaxnl_prb74", "phy_ulmaxnl_prb75", "phy_ulmaxnl_prb76",
						   "phy_ulmaxnl_prb77", "phy_ulmaxnl_prb78", "phy_ulmaxnl_prb79", "phy_ulmaxnl_prb80",
						   "phy_ulmaxnl_prb81", "phy_ulmaxnl_prb82", "phy_ulmaxnl_prb83", "phy_ulmaxnl_prb84",
						   "phy_ulmaxnl_prb85", "phy_ulmaxnl_prb86", "phy_ulmaxnl_prb87", "phy_ulmaxnl_prb88",
						   "phy_ulmaxnl_prb89", "phy_ulmaxnl_prb90", "phy_ulmaxnl_prb91", "phy_ulmaxnl_prb92",
						   "phy_ulmaxnl_prb93", "phy_ulmaxnl_prb94", "phy_ulmaxnl_prb95", "phy_ulmaxnl_prb96",
						   "phy_ulmaxnl_prb97", "phy_ulmaxnl_prb98", "phy_ulmaxnl_prb99"]],
			  'TM00002': [],
			  'TM00036': ['site_info', []]}


# 可实现对递归文件夹的监控
class FileEventHandler(FileSystemEventHandler):
	def __init__(self):
		FileSystemEventHandler.__init__(self)

	def on_created(self, event):

		if event.is_directory:
			print("directory created:{0}".format(event.src_path))
		else:
			print("file created:{0}".format(event.src_path))
			print('src_path = ', event.src_path)

			if (check(event.src_path)):
				# print('合规文件!')
				queue.put(event.src_path)
			print("queue size = ", queue.qsize())


def check(file_path):
	file_name, file_extension = os.path.splitext(os.path.basename(file_path))
	print('file_name = ', file_name)
	print('file_extension = ', file_extension)
	if (file_extension == '.csv' and file_name.find('TM') >= 0):
		return True
	else:
		return False


def fs_monitor(file_path):
	observer = Observer()
	event_handler = FileEventHandler()
	observer.schedule(event_handler, file_path, True)
	observer.start()
	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()


class Producer(threading.Thread):
	def run(self):
		fs_monitor(MONITIOR_FOLDER)


class Consumer(threading.Thread):
	def run(self):
		while 1:
			file_path = queue.get()
			# file_name,file_extension = os.path.splitext(os.path.basename(file_path))
			# print('file_extension = ',file_extension)
			# if file_extension != '.csv' or file_name not in table_dict.keys():
			#     print('不合规文件!')
			#     continue
			print("执行消费消费,开始解析:", file_path)
			try:
				read_csvfile(file_path)
			except Exception as e:
				print(e)
				# 如果之前操作失败,出现异常之前的语句会不会插入
				read_csvfile(file_path)

			print("解析入库数据完毕!,删除",file_path)
			if 'Linux' == platform.system():
				print("你的电脑是Linux！")
				os.remove(file_path)
			elif 'Darwin' == platform.system():
				print("你的电脑是MACOS！")
				os.remove(file_path)
			elif 'Windows' == platform.system():
				print("你的电脑是Win！")
				os.remove(file_path.replace(r"\\", "/"))
			else:
				print("你的电脑是其他系统！")


"""
解析
"""


def read_csvfile(file_path):
	# names 用于结果的列名列表，结合header=None
	file_name = os.path.basename(file_path)
	table_name = table_dict[file_name[0:7]][0]
	file_columns = table_dict[file_name[0:7]][1]

	# print('tablename:', table_name)
	# print("filecolumns:", file_columns)

	try:
		df = pd.read_csv(file_path, names = file_columns, encoding = 'utf8', sep = '|', header = None)
		print(df)
	except Exception as e:
		print(e)

	start1 = time.time()
	transprt_mysql(df, table_name)
	print("run cost:", time.time() - start1)


"""
入库
"""


def transprt_mysql(df, tablename):
	try:
		# 两句插入语句都能完成插入!
		# df.to_sql('cinma', con=yconnect, if_exists='append', index=False)
		pd.io.sql.to_sql(df, tablename, yconnect, if_exists = 'append', index = False)
		print()
	except Exception as e:
		print(e.message)


def main():
	# 实时监听生产
	p = Producer()
	p.start()

	# 消费 解析 入库
	for i in range(5):
		c = Consumer()
		c.start()


if __name__ == '__main__':
	main()
