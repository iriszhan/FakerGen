# FakerGen
A tool to create fake data.
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
