import sys
import uuid
from cio.CacheVariable import CacheValue
from cio.Configuration import readConfig
from process.Produce import getDatafromItemID
from process.Consumer import getResult
from util.RedisUtil import RedisHelper
from util.SqlalchemyUtil import SQLUtil
from util.LogUtil import MyLog
from bean.SpecialCharactersInfo import StrReplaceInfo
from common.DataInOut import DataIn, DataOut


def run(item_id: str):
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
    task_id = uuid.uuid4()
    similarthreshold: float = base_config["similarthreshold"]
    startmp_threshold: int = base_config["startmp_threshold"]
    expireTime: int = base_config["expireTime"]
    core_process: int = base_config["core_process"]
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
    # （1）先清除单条表数据
    conn_db.trucateTable(readConfig()["tablename"]["MaterialSingleResult"])
    # （2）替换字符
    SRIList = input_busisness.mysqlFileData(StrReplaceInfo)  # 替换字符
    # （3）批量数据：单条数据来源ItemID输入
    BUIListIsDigit, BUIListNoDigit, query_milist = getDatafromItemID(input=input, output=output,task_id=task_id,item_id=item_id ,SRIList=SRIList,
                                                                     startmp_threshold=startmp_threshold,
                                                                     core_process=core_process,mbr="MaterialSingleResult")

    print("BUIListIsDigit: {}; BUIListNoDigit: {};query_milist:{}".format(len(BUIListIsDigit), len(BUIListNoDigit),
                                                                          len(query_milist)))

    # ------------------------------------------------------------------------------------------------
    # TODO 3- 计算相似度 CalculateSimlary 并输出结果 Data.DataOut ————进行了多进程优化
    if (len(BUIListIsDigit) == 0 and len(BUIListNoDigit) == 0 and len(query_milist) == 0):
        mylog.error("输入的数据为None")
        return 0
    else:
        try:
            if (len(BUIListIsDigit) != 0 or len(BUIListNoDigit) != 0):
                # （4）主量数据
                MUIListIsDigit, MUIListNoDigit = CacheValue(input=input, SRIList=SRIList, conn_db=conn_db,
                                                            redisUtil=redisUtil,
                                                            expireTime=expireTime)
                print("MUIListIsDigit: {}; MUIListNoDigit: {}".format(len(MUIListIsDigit), len(MUIListNoDigit)))
                if len(BUIListIsDigit) > 0:  # 数值型
                    getResult(output, BUIListIsDigit, MUIListIsDigit, similarthreshold, core_process, startmp_threshold,
                              mbr="MaterialSingleResult")
                if len(BUIListNoDigit) > 0:  # 非数值型
                    getResult(output, BUIListNoDigit, MUIListNoDigit, similarthreshold, core_process, startmp_threshold,
                              mbr="MaterialSingleResult")
                if len(query_milist) > 0:
                    pass
            return 1
        except Exception as e:
            mylog.error("getResult Failed:{}".format(e))
            return 2

if __name__ == '__main__':
    # item_id: str = "220000002"
    # res = run(item_id)
    run(sys.argv[1])
