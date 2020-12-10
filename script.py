import threading
import time
import pika
import sys
from setting import *

lock=threading.Lock()
class Client(object):
    def __init__(self,host,message,
                 rout_key=ROUT_KEY,filename=FILENAME,
                 queue_name=MQ_NAME,exchange=EXCHANGE,
                 username=USERNAME,passwd=PASSWD):
        self.__message = message
        self.__host = host
        # self.__rout = rout_key
        self.__filename = filename
        self.__username = username
        self.__queue_name = queue_name
        # self.__exchange = exchange
        self.__passwd = passwd

    # 设置参数
    def add_pars(self):
        # 添加用户名和密码
        credentials = pika.PlainCredentials(self.__username, self.__passwd)
        # 配置连接参数
        parameters = pika.ConnectionParameters(host=self.__host, credentials=credentials)
        return parameters

    # 连接mq队列
    def connect_mq(self,parameters,count):
        try:
            # 加锁，防止竞争条件
            # 创建一个连接对象  
            
            connection = pika.BlockingConnection(parameters)
            lock.acquire()     
            count[0]+=1
            lock.release()      
            print(count)      

        except Exception as e:
            print(e)
            time2=time.time()
            timed=time2-time1
            print(timed)
            sys.exit(0)
        else:
            return connection

    # 创建信道
    def channel_mq(self,connection,prefetch=1):
        try:
            channel = connection.channel()
            # 公平调度
            channel.basic_qos(prefetch_count=prefetch)
        except Exception as e:
            print(e)
        else:
            return channel

    # 声明一个队列
    def queue_mq(self,channel,durable=True):
        try:
            result = channel.queue_declare(queue=self.__queue_name,durable=durable)
            print(result)
        except Exception as e:
            print(e)

    # 交换机队列绑定
    def exchange_queue(self,channel):
        try:
            channel.queue_bind(exchange=self.__exchange,
                               queue=self.__queue_name,
                               routing_key=self.__rout)
        except Exception as e:
            print(e)

    # 投递一条消息
    def message_mq(self,channel):
        try:
            result = channel.basic_publish(exchange=self.__exchange,
                                  routing_key=self.__rout,
                                  body=self.__message)
            if result:
                print(result)
        except Exception as e:
            print(e)

    # 创建一个直连交换机
    def direct_exchange(self,channel,exchange_name):
        try:
            channel.exchange_declare(exchange=exchange_name,
                                     exchange_type='direct')
        except Exception as e:
            print(e)

    # 创建一个主题交换机
    def topic_exchange(self,channel,exchange_name):
        try:
            channel.exchange_declare(exchange=exchange_name,
                                     exchange_type='topic',
                                     durable=False)
        except Exception as e:
            print(e)

    @staticmethod
    def open_data(filename):
        try:
            with open(filename,'r',encoding='utf-8') as f:
                data = f.read()
                return data
        except Exception as e:
            print(e)

# 测试时间装饰器
def test_nums(num=100):
    def test_time(func):
        def run(*args,**kwargs):
            # time1 = time.time()
            for i in range(num):
                 #print(i)
                 func(*args,**kwargs)
            # time2 = time.time()
            # print(num / (time2 - time1))
        return run
    return test_time


# 线程测试函数
def test_threads(func,nums,*args):
    for i in range(nums):   # 21*5/s
        thread = threading.Thread(target=func,args=args)
        thread.start()

        #print(i)

# 测试创建1000条tcp连接的速度
@test_nums(1)
def test_tcp(client,parameters,count):
    client.connect_mq(parameters,count)

# 测试创建1000个信道的速度
@test_nums(100)
def test_channel(client,con):
    client.channel_mq(con)

# 测试声明1000个队列的速度
@test_nums(1000)
def test_queue(client,channel):
    client.queue_mq(channel)

# 测试声明1000个直连交换机的速度
@test_nums(100)
def test_exchange(client, channel,name):
    client.direct_exchange(channel,name)

# 测试声明1000个主题交换机的速度
@test_nums(100)
def test_topic_exchange(client, channel,name):
    client.topic_exchange(channel,name)

# 测试绑定交换机和队列的速度
@test_nums(100)
def test_exchange_bind_queue(client, channel):
    client.exchange_queue(channel)

# 测试投递消息的速度
@test_nums(100)
def test_message(client, channel):
    client.message_mq(channel)

#
if __name__ == '__main__':
    time1=time.time()
    count=[0]
    message = Client.open_data(FILENAME)
    client = Client(HOST,message)
    pars = client.add_pars()
    # con = client.connect_mq(pars)
    # channel = client.channel_mq(con)
    #  client.direct_exchange(channel,'abc')
    #  client.topic_exchange(channel,'topic_eeg')

    # 测试连接tcp的速度
    # test_tcp(client,pars,count)   # 15/s
    test_threads(test_tcp, 2000, client,pars,count)   # 21*5/s
    # 测试创建信道的速度
    # test_channel(client,con)    # 27/s
    # test_threads(test_channel, 5, client,con)   # 21*5/s

    # 测试创建队列的速度
    # test_queue(client,channel)    # 55000msg/s
    # test_threads(test_queue, 5, client,channel)   # 30000/s

    # 测试创建直连交换机的速度
    # test_exchange(client,channel,'direct_eeg')  # 单线程50msg/s
    # test_threads(test_exchange, 5, client,channel,'direct_eeg')    # 多线程10*5msg/s

    # 测试创建主题交换机的速度
    # test_topic_exchange(client,channel,'topic_eeg')  # 50/s
    # test_threads(test_topic_exchange, 5, client,channel,'topic_eeg')    # 多线程10*5msg/s

    # 测试绑定的速度
    # test_exchange_bind_queue(client,channel)  # 55/s
    # test_threads(test_exchange_bind_queue, 5, client,channel)    # 多线程10*5msg/s

    # 测试投递消息的速度
    # test_message(client,channel)  # 12msg/s