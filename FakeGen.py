#!/usr/bin/env python
# encoding: utf-8
'''
@author: ZhanChen
@contact: 18629964611@163.com
@file: FakeGen.py
@time: 2019-08-16
@Description:用于生成用于测试的假数据，此版本支持的功能：
        1.生成基本数据类型数据（randint,randn,poisson,uniform,binomial,personName,company,job,province,dateTime,date）
        2.自定义列根据用户上传第一行为字段名称、下面为数据的.xlsx文件来生成数据
        3.根据现有数字列生成随机倍数的数，可通过设置倍数范围获得大于小于或成固定比例的数据
        4.根据现有时间列生成之前的时间，或生成之后的时间
        5.根据现有的列打乱后得到新列
        6.除用户上传的自定义列，其他列都可以通过设置missingRate来控制空值的比例
        7.如果用户同时定义了省（province）和市(city)，会保证省与市相对应，但如果是用户后续对省市执行了打乱功能生成的依赖列，
          此脚本没支持省市对应
        8.用户可以配置一一对应的列名，设置成假的一一对应（即不考虑值的意义，只是能保证形式上的一一对应，
          此功能最好不要用在province,city字段上，否则会造成province,city不再匹配）
@Note:一定要把读入的用户自定义的数据读为名字为diyDT的pandas.DataFrame，因为gen_data函数里用到了diyDT的名字
'''

import random
from faker import Faker
import pandas as pd
import numpy as np

#配置生成数据为中文
f=Faker(locale='zh_CN')

#省市字典
provinceCityDict={"直辖市":["北京","上海","天津","重庆"],"特别行政区":["香港","澳门","台湾"],"辽宁省":["沈阳市","大连市","鞍山市","抚顺市","本溪市","丹东市","锦州市","葫芦岛市","营口市","盘锦市","阜新市","辽阳市","铁岭市","朝阳市","凌源市","北票市"],
                 "吉林省":["长春市","吉林市","四平市","辽源市","通化市","白山市","延边市","白城市","松原市"],"黑龙江省":["哈尔滨市","齐齐哈尔市","鹤岗市","双鸭山市","鸡西市","大庆市","伊春市","牡丹江市","佳木斯市","七台河市","黑河市","绥化市","大兴安岭地区"],
                 "河北省":["石家庄市","唐山市","秦皇岛市","邯郸市","邢台市","保定市","张家口市","承德市","廊坊市","衡水市","沧州市"],
                 "山西省":["太原市","大同市","阳泉市","长治市","晋城市","朔州市","晋中市","运城市","忻州市","临汾市","吕梁地区"],
                 "河南省":["郑州市","开封市","洛阳市","平顶山市","焦作市","鹤壁市","新乡市","安阳市","濮阳市","许昌市","漯河市","三门峡市","南阳市","商丘市","信阳市","周口市","驻马店市","济源市"],
                 "山东省":["济南市","青岛市","淄博市","枣庄市","东营市","潍坊市","烟台市","威海市","济宁市","泰安市","日照市","莱芜市","临沂市","德州市","聊城市","滨州市","菏泽市"],
                 "江苏省":["南京市","徐州市","连云港市","淮安市","宿迁市","盐城市","扬州市","泰州市","南通市","镇江市","常州市","无锡市","苏州市"],
                 "安徽省":["合肥市","芜湖市","蚌阜市","淮南市","马鞍山市","淮北市","铜陵市","安庆市","黄山市","滁州市","阜阳市","宿州市","巢湖市","六安市","毫州市","池州市","宣城市"],
                 "江西省":["南昌市","萍乡市","九江市","新余市","鹰潭市","赣州市","吉安市","宜春市","抚州市","上饶市"],
                 "浙江省":["杭州市","宁波市","温州市","嘉兴市","绍兴市","金华市","衢州市","舟山市","台州市","丽水市"],
                 "福建省":["福州市","厦门市","三明市","莆田市","泉州市","漳州市","南平市","龙岩市","宁德市"],
                 "广东省":["广州市","深圳市","珠海市","汕头市","韶关市","惠州市","河源市","梅州市","汕尾市","东莞市","中山市","江门市","佛山市","阳江市","湛江市","茂名市","清远市","潮州市","揭阳市","云浮市"],
                 "海南省":["海口市","三亚市"],
                 "贵州省":["贵阳市","六水盘市","遵义市","安顺市","铜仁地区","毕节地区"],
                 "云南省":["昆明市","玉溪市","保山市","昭通市","思茅地区","临沧地区"],
                 "四川省":["成都市","自贡市","攀枝花市","泸州市","德阳市","绵阳市","广元市","遂宁市","内江市","乐山市","南充市","宜宾市","广安市","达州市","眉山市","雅安市","巴中市","资阳市","阿坝藏族羌族自治州","甘孜藏族自治州","凉山彝族自治州"],
                 "湖南省":["长沙市","株洲市","湘潭市","衡阳市","邵阳市","岳阳市","常德市","张家界市","益阳市","郴州市","永州市","怀化市","娄底市","湘西土家苏苗族自治州"],
                  "湖北省":["武汉市","黄石市","襄樊市","十堰市","荆州市","宜昌市","荆门市","鄂州市","孝感市","黄冈市","咸宁市","随州市","施恩特家族苗族自治州","仙桃市","天门市","潜江市"],
                 "陕西省":["西安市","铜川市","宝鸡市","咸阳市","渭南市","延安市","汉中市","榆林市","安康市","商洛市"],
                 "甘肃省":["兰州市","金昌市","白银市","天水市","嘉峪关市","武威市","张掖市","平凉市","酒泉市","庆阳市","定西","陇南","甘南藏族自治州","临夏回族自治州"],
                 "青海省":["西宁市","海东地区","海北藏族自治州","黄南藏族自治州","海南藏族自治州","果洛藏族自治州","玉树藏族自治州","海西蒙古自治州"],
                  "内蒙古自治区":["呼和浩特市","包头市","乌海市","赤峰市","鄂尔多斯市","呼伦贝尔市","乌兰察布盟","锡林郭勒盟","巴彦卓尔盟","阿拉善盟"],
                  "西藏自治区":["拉萨市","昌都地区","山南地区","日喀则地区","阿里地区","林芝地区"],
                  "新疆维吾尔自治区":["乌鲁木齐市","克拉玛依市","吐鲁番地区","哈密地区","和田地区","阿克苏地区","喀什地区","克孜勒苏柯尔克孜自治州","巴音郭勒州","昌吉州","博尔塔拉州","伊犁哈萨克自治州","塔城地区","阿勒泰地区","石河子市","阿拉尔市","图木舒克市","五家渠市"],
                  "广西壮族自治区":["南宁市","柳州市","桂林市","梧州市","北海市","防城港市","钦州市","贵港市","玉林市","百色市","贺州市","河池市","来宾市","崇左市"],
                  "宁夏回族自治区":["银川市","石嘴山市","吴忠市","固原市"]
                 }
# 基础列配置部分
initDict = {}#都保存在initDict


def randint_init(name, missingRate, high, low):
    initDict[name] = {"type": "randint", "missingRate": missingRate, "high": high, "low": low}


def randn_init(name, missingRate, mu, sigma):
    initDict[name] = {"type": "randn", "missingRate": missingRate, "mu": mu, "sigma": sigma}


def poisson_init(name, missingRate, lamb):
    initDict[name] = {"type": "poisson", "missingRate": missingRate, "lambda": lamb}


def uniform_init(name, missingRate, low, high):
    initDict[name] = {"type": "uniform", "missingRate": missingRate, "low": low, "high": high}


def binomial_init(name, missingRate, n, p):
    initDict[name] = {"type": "binomial", "missingRate": missingRate, "n": n, "p": p}


def personName_init(name, missingRate):
    initDict[name] = {"type": "personName", "missingRate": missingRate}


def company_init(name, missingRate):
    initDict[name] = {"type": "company", "missingRate": missingRate}


def job_init(name, missingRate):
    initDict[name] = {"type": "job", "missingRate": missingRate}


def province_init(name, missingRate):
    initDict[name] = {"type": "province", "missingRate": missingRate}


def city_init(name, missingRate):
    initDict[name] = {"type": "city", "missingRate": missingRate}


def dateTime_init(name, missingRate, start, end):
    initDict[name] = {"type": "dateTime", "missingRate": missingRate, "start": start, "end": end}


def date_init(name, missingRate, start, end):
    initDict[name] = {"type": "date", "missingRate": missingRate, "start": start, "end": end}

#检查用户是否同时选择了“省份”和“城市”，如果同时选择，“城市”的选择将基于“省份”；若不是同时选择，无约束生成。
def check_city(myInitDict):
    provinceFlag=0
    provinceColName=None
    for colname in myInitDict:
        if myInitDict[colname]["type"]=="province":
            provinceFlag=1
            provinceColName=colname#记录province所在列的名字
    if provinceFlag==1:#有省份字段
        for colname in myInitDict:
            if myInitDict[colname]["type"]=="city":
                myInitDict[colname]["type"]="city_dep"#更改城市的类型为city_dep
                myInitDict[colname]["dep"]=provinceColName#记录依赖的列名
    return myInitDict

#自定义列部分
#用户上传一个excel，第一行为字段名，后面为每个字段的值
#限制：字段名和已有的基础列名不能相同，且上传的excel第一行不能有重复值。
#上传的excel读成pd.DataFrame
def readExcel(dir):
    """
    读存在dir的excel
    """
    return pd.read_excel(dir)

def diy_init(df,related):
    """
    df:数据框
    related:各列是否相关（True:相关,False:不相关）
    """
    for col in range(len(df.columns)):
        initDict[df.columns.values[col]]={"type":"DIY","related":related}

#例如自定义的数据上传后读成了dt,生成'type'为'DIY'的列的时候从diyDT里随机选
diyDT=pd.DataFrame({"a":[1,2,1,0],"b":[3,3,3,3]})

# 依赖列配置

# 生成的新列是已有列的倍数
# 用户指定倍数范围
# 1.如果希望比例都是一样的，等价于比例最小值和最大值相等；
# 2.如果想要数字比原来大，等价于low和high都为大于1的数
# 3.如果想要数字比原来小，等价于low和high都为小于1的数
# 适用于依赖的列的类型属于（randint,randn,poisson,uniform,binomial），其他类型不行。
def per_dep_init(name, missingRate, low, high, dep):
    """
    name:新列的名字
    missingRate:缺失比例
    low:倍数的最小值
    high:倍数的最大值
    dep:依赖列的名字
    return:无。在原有的配置字典initDict中添加一项
    """
    initDict[name] = {"type": "per_dep", "missingRate": missingRate, "low": low, "high": high, "dep": dep}


# 生成的新列的时间是已有列的时间（datetime）之后
# 用户指定列名
# 适用于依赖的列的类型是(dateTime)
def dateTime_dep_init(name, missingRate, dep, future):
    """
    name:新列的名字
    missingRate:缺失比例
    dep:依赖列的名字
    future:bool类型，1为依赖列时间之后，0为依赖列时间之前
    return:无。在原有的配置字典initDict中添加一项
    """
    initDict[name] = {"type": "dateTime_dep", "missingRate": missingRate, "dep": dep, "future": future}


# 生成新列的日期是已有列的时间（date）之后
# 用户指定列名
# 适用于依赖的列的类型是（date）
def date_dep_init(name, missingRate, dep, future):
    """
    name:新列的名字
    missingRate:缺失比例
    dep:依赖列的名字
    future:int类型，1为依赖列时间之后，0为依赖列时间之前
    return:无。在原有的配置字典initDict中添加一项
    """
    initDict[name] = {"type": "date_dep", "missingRate": missingRate, "dep": dep, "future": future}


# 生成的新列是已有列的随机选取
# 适用于依赖的列的类型可以为任意类型，但是province，city这种有隐含对应关系的最好不要用
def shuffle_dep_init(name, missingRate, dep):
    """
    name:新列的名字
    missingRate:缺失比例
    return:无。在原有的配置字典initDict中添加一项
    """
    initDict[name] = {"type": "shuffle_dep", "missingRate": missingRate, "dep": dep}

#数据整体配置部分
data_initDict={}#整体配置记录在这里
def data_init(num,outputType,encoding):
    data_initDict["num"]=num#配置生成数据的量
    data_initDict["outputType"]=outputType#配置文件的输出类型
    data_initDict["encoding"]=encoding#配置文件的编码

#其他函数部分
def missing(dataSeries,missing_rate=0.1):
    """
    dataSeries:需要添加缺失值的pd.Series
    missing_rate:缺失值的比例，默认为0.1
    return:缺失率为missing_rate的pd.Series
    """
    seriesLength=dataSeries.shape[0]
    missingLength=int(seriesLength*missing_rate)
    index=list(range(seriesLength))
    np.random.shuffle(index)
    missingIndex=index[:missingLength]
    dataSeries[missingIndex]=None
    return dataSeries

#faker包里的省份名有的不对，比如“黑龍江省”，所以另外写一个函数
def get_provinceName(df,valueDict):
    return random.choice(list(valueDict.keys()))

def get_provinceName_series(num):
    return pd.Series([None]*num).apply(get_provinceName,args=(provinceCityDict,))

#对于依赖列的处理函数
def dep(df,depend,valueDict):
    try:
        return random.choice(valueDict[df[depend]])
    except:
        None

def get_series(func=f.name,num=5):
    """
    func:Faker函数包中的函数名作为参数传入，默认为f.name
    num:想要得到的pd.Series的长度，默认为5
    return:Series
    """
    return pd.Series([None]*num).apply(lambda x:func())

#生成数据部分
def gen_data(initDict, data_initDict):
    # 首先创建空数据框
    colnameList = list(initDict.keys())
    dt = pd.DataFrame(columns=colnameList)
    num = data_initDict['num']
    cumulate = None
    # typeList=[initDict[col]["type"] for col in colnameList]
    # if "province" in typeList:
    # provinceFlag=1#记录是否有province字段
    # else:
    # provinceFlag=0
    for col in colnameList:
        colType = initDict[col]["type"]
        if colType == "randint":
            dt[col] = np.random.randint(low=initDict[col]["low"], high=initDict[col]["high"], size=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "randn":
            mu = initDict[col]["mu"]
            sigma = initDict[col]["sigma"]
            dt[col] = mu + sigma * np.random.randn(num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "poisson":
            lamb = initDict[col]["lambda"]
            dt[col] = np.random.poisson(lam=initDict[col][lamb], size=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "uniform":
            low = initDict[col]["low"]
            high = initDict[col]["high"]
            dt[col] = np.random.uniform(low=low, high=high, size=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "binomial":
            n = initDict[col]["n"]
            p = initDict[col]["p"]
            dt[col] = np.random.binomial(n=n, p=p, size=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "personName":
            dt[col] = get_series(func=f.name, num=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "company":
            dt[col] = get_series(func=f.company, num=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "province":
            dt[col] = get_provinceName_series(num=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "city":
            dt[col] = get_series(func=f.city, num=num)
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "dateTime":
            dt[col] = pd.Series([None] * num).apply(
                lambda x: f.date_time_between(start_date=initDict[col]["start"], end_date=initDict[col]["end"]))
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "date":
            dt[col] = pd.Series([None] * num).apply(
                lambda x: f.date_time_between(start_date=initDict[col]["start"], end_date=initDict[col]["end"]).date())
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])

        # 用户自定义的列
        elif colType == "DIY":
            related = initDict[col]["related"]  # 是否是各列相关的，如果是False，可以单独抽样；如果为True，需要合并起来，一起抽样。
            if related:
                if cumulate is None:
                    cumulate = pd.DataFrame(diyDT[col])
                else:
                    cumulate[col] = diyDT[col]
            else:
                dt[col] = np.array(dt[col].sample(n=dt.shape[0], replace=True, axis=0))

    if cumulate is not None:
        dt[list(diyDT.columns.values)] = np.array(cumulate.sample(n=dt.shape[0], replace=True, axis=0))

    # 等上面的基本列都填充完毕，来填充依赖列
    for col in colnameList:
        colType = initDict[col]["type"]
        if colType == "city_dep":
            dependColName = initDict[col]["dep"]  # 依赖的列名
            dt[col] = dt.apply(func=dep, axis=1, args=(dependColName, provinceCityDict))
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "per_dep":
            dependColName = initDict[col]["dep"]
            low = initDict[col]["low"]
            high = initDict[col]["high"]
            dt[col] = np.array(dt[dependColName]) * np.random.uniform(low=low, high=high, size=dt.shape[0])
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "shuffle_dep":
            dependColName = initDict[col]["dep"]
            dt[col] = np.array(dt[dependColName].sample(n=dt.shape[0], replace=True))
            dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
        elif colType == "dateTime_dep":
            dependColName = initDict[col]["dep"]
            future = initDict[col]["future"]  # 0或1，对应之前或之后
            if future == 1:
                dep_end = initDict[dependColName]["end"]
                dt[col] = pd.Series([None] * num).apply(lambda x: f.date_time_between(start_date=dep_end))
                dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])
            else:
                dep_start = initDict[dependColName]["start"]
                dt[col] = pd.Series([None] * num).apply(lambda x: f.date_time_between(end_date=dep_start))
                dt[col] = missing(dt[col], missing_rate=initDict[col]["missingRate"])

    return dt

#指定两列是一对一的关系。（这种一对一的关系不考虑实际是否有一对一的关系，因为都是假数据，只是说这两列的两两组合只能是一种，不能随意组合）
#方法：在生成的数据框中每列分别去重，得到的数据框的的每行都是这两列的一一对应的组合，再重新在其中挑选填充
#省份，城市不能允许定义一对一，有时分开筛选，就乱了。不能让用户选择有省份城市的选项。
#用户输入：有一一对应关系的两个字段名，保存成元组列表
oneToOne_init=[]
def oneToOne(colName1,colName2):
    oneToOne_init.append((colName1,colName2))

#解析oneToOne_init
def get_oneToOne(oneToOne_init,dt):
    """
    oneToOne_init:用户指定的含有一对一关系的列表
    dt:需要修改的数据框
    return:修改后的数据
    """
    #for oneToOneTuple in oneToOne_init:
        #replace=dt.loc[:,[oneToOneTuple[0],oneToOneTuple[1]]].drop_duplicates().sample(n=dt.shape[0],replace=True,axis=0)
        #dt.loc[:,[oneToOneTuple[0]]]=np.array(replace.loc[:,[oneToOneTuple[0]]])
        #dt.loc[:,[oneToOneTuple[1]]]=np.array(replace.loc[:,[oneToOneTuple[1]]])
    #return dt
    for oneToOneTuple in oneToOne_init:
        s1=pd.DataFrame(dt[oneToOneTuple[0]]).drop_duplicates()
        s2=pd.DataFrame(dt[oneToOneTuple[1]]).drop_duplicates()
        sShort=len(s1) if len(s1)<len(s2) else len(s2)
        shuffle=pd.DataFrame({oneToOneTuple[0]:s1[:sShort].values.reshape(-1),oneToOneTuple[1]:s2[:sShort].values.reshape(-1)}).sample(n=dt.shape[0],replace=True)
        dt.loc[:,[oneToOneTuple[0]]]=np.array(shuffle.loc[:,[oneToOneTuple[0]]])
        dt.loc[:,[oneToOneTuple[1]]]=np.array(shuffle.loc[:,[oneToOneTuple[1]]])
    return dt

#测试部分
if __name__=="__main__":
    #配置部分例子
    #1.基础列配置
    randint_init("randint", 0.3, 10, 3)
    personName_init("person", 0.1)
    province_init("province", 0)
    city_init("city", 0)
    diy_init(df=diyDT, related=1)
    dateTime_init(name="进货时间", missingRate=0, start='-10d', end='-2d')
    #2.依赖列配置
    per_dep_init(name="perdep", missingRate=0, low=1, high=2.5, dep="randint")
    shuffle_dep_init(name="shuffledep", missingRate=0, dep="city")
    dateTime_dep_init(name="卖出时间", missingRate=0, dep="进货时间", future=1)
    #3.自定义列读取
    diyDT=readExcel('./diyData/test.xlsx')
    #4.整体数据配置
    data_init(num=10, outputType="csv", encoding="gbk")

    # 把check_city后的initDict重新赋值给initDict
    initDict = check_city(initDict)

    #配置字段名为person和randint的两个字段有一对一的关系
    oneToOne_init = []
    oneToOne("person", "randint")

    #输出没进行一对一处理的结果
    dt1=gen_data(initDict, data_initDict)
    print(dt1)

    #输出一对一处理后的结果
    dt2=get_oneToOne(oneToOne_init, dt1)
    print(dt2)

    #导出数据
    dt1.to_excel('./result/result.xlsx')
    dt2.to_excel('./result/resultOneToOne.xlsx')
