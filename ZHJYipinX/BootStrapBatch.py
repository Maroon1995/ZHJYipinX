import sys
from ZHJYipinX.cio.CacheVariable import Caches
from ZHJYipinX.cio.Configuration import readConfig
from ZHJYipinX.process.Produce import getDatafromDB
from ZHJYipinX.process.Consumer import getResult
from ZHJYipinX.util.RedisUtil import RedisHelper
from ZHJYipinX.util.SqlalchemyUtil import SQLUtil
from ZHJYipinX.util.LogUtil import MyLog
from ZHJYipinX.bean.SpecialCharactersInfo import StrReplaceInfo
from ZHJYipinX.bean.BatchMaterialInfo import BatchMaterial
from ZHJYipinX.common.DataInOut import DataIn, DataOut
from ZHJYipinX.common.ResumeTime import get_time

@get_time
def run(soid: int):
    """
    :param task_id: 任务id
    :param task_data: 接收到的任务数据
    :return:
    """
    # ------------------------------------------------------------------------------------------------
    # TODO 0- 配置文件
    cfg = readConfig()
    base_config = cfg["basevariable"]
    redis_config = cfg["database"]["redis"]["config"]
    mssql_config = cfg["database"]["mssql"]
    mssql_config_busisness = cfg["database"]["busisness_mssql"]
    # TODO 1- 参数设置
    tabelname = BatchMaterial
    similarthreshold: float = base_config["similarthreshold"]
    startmp_threshold: int = base_config["startmp_threshold"]
    expireTime: int = base_config["expireTime"]
    core_process: int = base_config["core_process"]
    filterCondition = BatchMaterial.soid == soid
    db_path = mssql_config["url"]  # mssql 数据库的url
    db_path_busisness = mssql_config_busisness["url"]
    # ----------------------------------------------------------------------------------------------
    # TODO 2- 实例化对象
    conn_db = SQLUtil(path=db_path)
    redisUtil = RedisHelper(redis_config["host"], redis_config["port"], redis_config["db"], redis_config["pwd"])
    input = DataIn(db_path)
    input_busisness = DataIn(db_path_busisness)
    output = DataOut(db_path)
    mylog = MyLog()
    # ------------------------------------------------------------------------------------------------
    # TODO 3- 获取数据 getData（包含了数据处理）————进行了缓存优化
    # （1）替换字符
    SRIList = input_busisness.mysqlFileData(StrReplaceInfo)  # 替换字符
    # （2）批量数据：批量数据来源DB
    BUIListIsDigit, BUIListNoDigit, query_milist = getDatafromDB(input=input, output=output, tabelname=tabelname,
                                                                 SRIList=SRIList, filterCondition=filterCondition,
                                                                 startmp_threshold=startmp_threshold,
                                                                 core_process=core_process,mbr= "MaterialBatchResult")

    print("BUIListIsDigit: {}; BUIListNoDigit: {};query_milist:{}".format(len(BUIListIsDigit), len(BUIListNoDigit),
                                                                          len(query_milist)))
    # ------------------------------------------------------------------------------------------------
    # TODO 3- 计算相似度 CalculateSimilar 并输出结果 Data.DataOut ————进行了多进程优化
    if (len(BUIListIsDigit) == 0 and len(BUIListNoDigit) == 0 and len(query_milist) == 0):
        mylog.error("输入的数据为None")
        return 0
    else:
        try:
            if(len(BUIListIsDigit) != 0 or len(BUIListNoDigit) != 0):
                # 获取主量数据并缓存
                cache = Caches(expireTime=expireTime, SRIList=SRIList)
                cache.input = input
                cache.conn_db = conn_db
                cache.redisUtil = redisUtil
                MUIListIsDigit, MUIListNoDigit = cache.CacheValue()

                print("MUIListIsDigit: {}; MUIListNoDigit: {}".format(len(MUIListIsDigit), len(MUIListNoDigit)))
                if len(BUIListIsDigit) > 0:  # 数值型
                    getResult(output, BUIListIsDigit, MUIListIsDigit, similarthreshold, core_process, startmp_threshold,
                          mbr="MaterialBatchResult")
                if len(BUIListNoDigit) > 0:  # 非数值型
                    getResult(output, BUIListNoDigit, MUIListNoDigit, similarthreshold, core_process, startmp_threshold,
                          mbr="MaterialBatchResult")
                if len(query_milist) > 0:
                    pass

            return 1
        except Exception as e:
            mylog.error("getResult Failed:{}".format(e))
            return 3


if __name__ == '__main__':
    # 任务soid
    # soid = 13074
    # res = run(soid)
    run(int(sys.argv[1]))
