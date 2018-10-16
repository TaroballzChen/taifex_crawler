import requests
import datetime,time
import csv
from bs4 import BeautifulSoup

url = "http://info512ah.taifex.com.tw/Future/FusaQuote_Norl.aspx"

goods = "臺指期108"
date = datetime.datetime.now().strftime("%Y%m%d")

def get_my_hope_data(goods_name=goods):
    data = requests.get(url)
    soup = BeautifulSoup(data.text, 'html.parser')
    tr_tags = soup.find_all("tr",class_ = "custDataGridRow")
    a = [tr_text.find_all("a")[0].string == goods_name for tr_text in tr_tags ]
    selected_information = [info[0] for info in zip(tr_tags,a) if info[1]==True][0]
    return selected_information

def split_data(data_selected):
    td_tags = data_selected.find_all("td")
    origin_data = [x.string for x in td_tags if x.string!=None]
    return origin_data

def operate_data(data):
    buy_price = data[0]
    buy_amout = data[1]
    sell_price = data[2]
    sell_amout = data[3]
    trade_price = data[4]
    up_down = data[5]
    Amplitude = data[6]
    trade_amout = data[7]
    star_price = data[8]
    max_price = data[9]
    min_price = data[10]
    ref_price = data[11]
    time = data[-1]
    return [goods,time,trade_price,buy_amout,trade_amout,max_price,min_price]

def data_write(fileName="./%s/%s_Match.csv"%(goods,date),):
    output_file = open (fileName,'a',newline='')
    writer = csv.writer(output_file)
    writer.writerow(["商品代號","時間","成交價","成交量","總量","最高價","最低價"])
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
    count = 0
    write = data_write()
    next(write)
    while 1:
        origindata = get_my_hope_data()
        o_data = split_data(origindata)
        print(o_data)
        sort_data = operate_data(o_data)
        write.send(sort_data)
        time.sleep(5)

