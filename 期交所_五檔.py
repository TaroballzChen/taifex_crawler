from urllib.request import urlretrieve
import requests.packages.urllib3
import pandas as pd
import time
import datetime
import csv


url = "http://info512ah.taifex.com.tw/Future/ImgChartDetail.ashx?type=1&contract=TX108"
goods = "臺指期108"
date = datetime.datetime.now().strftime("%Y%m%d")

def download_data():
    requests.packages.urllib3.disable_warnings()
    urlretrieve(url, filename="hosts.csv")

def read_data(fileName="./hosts.csv"):
    data = pd.read_csv(fileName,encoding="big5",delimiter='\t')
    data = data[goods]
    return(data)

def split_data(data):
    down = []
    up = []
    for i in range(13,18,1):
        five_data  = data[data.index==i].values[0].split(',')
        down.extend([five_data[0],five_data[1]])
        up.extend([five_data[-2],five_data[-1]])

    else:
        down.extend(up)
        return down

def get_time(data):
    t_time = data[data.index==11].values[0].split(',')[-1]
    return t_time

def data_write(fileName="./%s/%s_UpDn5.csv"%(goods,date),):
    output_file = open (fileName,'a',newline='')
    writer = csv.writer(output_file)
    writer.writerow(['商品代號', '時間', '下一檔價格', '下一檔數量', '下兩檔價格', '下兩檔數量', '下三檔價格', '下三檔數量', '下四檔價格', '下四檔數量', '下五檔價格', '下五檔數量', '上一檔價格', '上一檔數量', '上兩檔價格', '上兩檔數量', '上三檔價格', '上三檔數量', '上四檔價格', '上四檔數量', '上五檔價格', '上五檔數量'])
    output_file.close()
    record_time = 0
    yield
    while 1:
        write_data = yield
        if record_time == write_data[1]:
           record_time = write_data[1]
        else:
            output_file = open(fileName, 'a', newline='')
            writer = csv.writer(output_file)
            writer.writerow(write_data)
            record_time = write_data[1]
            output_file.close()

if __name__ == '__main__':
    write = data_write()
    next(write)
    while 1:
        download_data()
        data = read_data()
        t_time = get_time(data)
        UpDn5 = split_data(data)
        writeData = [goods,t_time]
        writeData.extend(UpDn5)
        print(t_time)
        write.send(writeData)
        time.sleep(5)