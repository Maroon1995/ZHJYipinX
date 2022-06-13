import json

from typing import List, Dict
from ZHJYipinX.common.CalculateVector import getVectorString
from ZHJYipinX.common.CalculateSymbol import getNoSymbol
from ZHJYipinX.common.DataInOut import DataIn
from ZHJYipinX.bean.MainMaterialUniformInfo import MainMaterialUniform
from ZHJYipinX.bean.SpecialCharactersInfo import StrReplaceInfo
from ZHJYipinX.util.RedisUtil import RedisHelper
from ZHJYipinX.util.SqlalchemyUtil import SQLUtil
from sqlalchemy import or_


class Caches(object):
    """
    :param input: 实例化的输出对象DataIn
    :param SRIList: 替换字符列表
    :param conn_db: DB数据库实例对象SQLUtil
    :param redisUtil: redis工具类实例
    :param expireTime: redis中数据有效期
    """
    redisUtil: RedisHelper
    conn_db: SQLUtil
    input: DataIn
    MUIListIsDigit: List = []
    MUIListNoDigit: List = []
    newList = []  # 存放status=1，2，需要被更新的MainMaterialUniform

    def __init__(self, expireTime: int, SRIList: List[StrReplaceInfo]):
        self.expireTime = expireTime
        self.SRIList = SRIList

    def _jsonStringToMMU(self, dicts: Dict):
        """
        将json字符串转换成字典，并重新封装成MainMaterialUniform:MMU实例对象
        :param stringdict: json字符串
        :return:
        """
        # 重新封装
        return MainMaterialUniform(dicts.get("PDM_ID"), dicts.get("ITEM_ID"), dicts.get("uniformitem"),
                                   dicts.get("ITEM_NAME_EN"), dicts.get("ITEM_NAME_CH"),
                                   dicts.get("ITEM_UNIT"), dicts.get("ONE_TYPE"),
                                   dicts.get("vector"), dicts.get("status"), dicts.get("ITEM_ID_ORIGINAL"),
                                   dicts.get("PEOPLE_ONLYITEM"), dicts.get("PEOPLE_NAME_CH"),
                                   dicts.get("PEOPLE_NAME_EN"), dicts.get("PEOPLE_UNIT"))

    def getIsOrNoDigitList(self, username: str, redisUtil: RedisHelper, expireTime: int):
        """
        读取redis缓存数据，并转换成List[MaterialUniform]
        :param username: redis中的哈希表名称
        :param redisUtil: redis工具类
        :return:
        """
        MUIListDict = redisUtil.getall_values_hset(username, expireTime)

        return list(map(lambda x: self._jsonStringToMMU(json.loads(x)), MUIListDict))

    def _writeToRedis(self, field, eleMUI):
        """
        # 添加数据到redis和newList中
        :param field: 字符串\图号\物料编码
        :param eleMUI: DB数据库中查询到的每条数据
        """
        new_data = json.dumps(dict(eleMUI))
        if field.isdigit():  # 分类出数值字符串和非数值字符串
            self.MUIListIsDigit.append(eleMUI)  # 添加到列表
            self.redisUtil.add_hset("MUIListIsDigit", eleMUI.ITEM_ID, new_data,
                                    self.expireTime)  # 添加到缓存哈希表“MUIListIsDigit”中
        else:
            self.MUIListNoDigit.append(eleMUI)
            self.redisUtil.add_hset("MUIListNoDigit", eleMUI.ITEM_ID, new_data, self.expireTime)

    def _split_add_hset(self, field, eleMUIStatus, new_data):
        """
        将数值型字符串和非数值型字符串分开存放到redis中去
        :param field: 需要区分的字符串\图号\物料编码
        :param eleMUIStatus: 读取到的每行数据
        :param new_data: 更新过向量的新数据
        """
        if field.isdigit():
            self.redisUtil.add_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID, new_data, self.expireTime)
        else:
            self.redisUtil.add_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID, new_data, self.expireTime)

    def _updateToRedis(self, field, eleMUIStatus):

        eleMUIStatus.status = 0  # 将状态更新成0，表示已经成为历史物料
        new_data = json.dumps(dict(eleMUIStatus))
        dicts = dict(eleMUIStatus)
        self.newList.append(self._jsonStringToMMU(dicts))  # 将更后的数据添加到等待同步到DB数据库的列表中

        if self.redisUtil.isExists_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID,
                                        self.expireTime) == 1:  # 如果存在,则先删除该key-value，再添加key_newvalue
            self.redisUtil.del_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID, self.expireTime)
            self._split_add_hset(field, eleMUIStatus, new_data)

        elif self.redisUtil.isExists_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID, self.expireTime) == 1:
            self.redisUtil.del_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID, self.expireTime)
            self._split_add_hset(field, eleMUIStatus, new_data)

        else: # 以上两种情况都不存在，则直接判断是否为数值型字符串，然后添加

            self._split_add_hset(field, eleMUIStatus, new_data)

    def CacheValue(self):
        """
        :return: MUIListIsDigit, MUIListNoDigit
        """
        filterCondition = or_(MainMaterialUniform.status == 1, MainMaterialUniform.status == 2)  # 状态条件，1表示新增，2表示删除
        # 查询MainMaterialUniform表中是否存在status为1的数据，若存在则更缓存redis
        flag: int = self.conn_db.mainKeyExists(MainMaterialUniform, filterCondition)
        digitExistsTrue = self.redisUtil.is_existsKey("MUIListIsDigit",self.expireTime) == True
        nodigitExistsTrue = self.redisUtil.is_existsKey("MUIListNoDigit", self.expireTime) == True

        if flag == 0 and digitExistsTrue and nodigitExistsTrue:  # 如果，两个username如果均存在且flag=0，则从redis中获取
            print("从redis中获取")
            self.MUIListIsDigit = self.getIsOrNoDigitList("MUIListIsDigit", self.redisUtil, self.expireTime)
            self.MUIListNoDigit = self.getIsOrNoDigitList("MUIListNoDigit", self.redisUtil, self.expireTime)

        elif flag == 1 and digitExistsTrue and nodigitExistsTrue:  # 如果，两个username均存在, 且flag=1，则更新redis中对应的值，然后再获取数据
            # 从DB中获取MainMaterialUniform中status == 1和2的所有数据
            MUIListStatus: List[MainMaterialUniform] = self.input.mysqlFileData(MainMaterialUniform,
                                                                                filterCondition=filterCondition)
            for i in range(len(MUIListStatus)):  # 更新数据
                eleMUIStatus = MUIListStatus[i]
                if eleMUIStatus.status == 1:
                    uniform = getNoSymbol(eleMUIStatus.PEOPLE_ONLYITEM, self.SRIList)  # 先对人工确认编码进行统一处理
                    eleMUIStatus.vector = getVectorString(uniform)  # 新增时：更新向量
                    self._updateToRedis(uniform, eleMUIStatus)  # 对redis中的数据进行更新

                if eleMUIStatus.status == 2:
                    eleMUIStatus.vector = getVectorString(eleMUIStatus.uniformitem)  # 删除时：更新向量
                    self._updateToRedis(eleMUIStatus.uniformitem, eleMUIStatus)

            print("从redis中获取")
            self.MUIListIsDigit = self.getIsOrNoDigitList("MUIListIsDigit", self.redisUtil, self.expireTime)
            self.MUIListNoDigit = self.getIsOrNoDigitList("MUIListNoDigit", self.redisUtil, self.expireTime)

        else:  # 否则，从DB数据库中获取,并重新更新redis中的数据
            print("从DB数据库获取,并更新数据")
            MUIList: List[MainMaterialUniform] = self.input.mysqlFileData(MainMaterialUniform)
            # 清除一次redis
            self.redisUtil.flushData("MUIListIsDigit")
            self.redisUtil.flushData("MUIListNoDigit")
            for i in range(len(MUIList)):
                eleMUI = MUIList[i]
                # -----------------------------------------------
                # 更新MainMaterialUniform中的向量vector，当状态status为1的时候
                if eleMUI.status == 1:  # 当状态为1时，说明该图号已经被确认过，需要对其进行一次更新向量的操作
                    uniform = getNoSymbol(eleMUI.PEOPLE_ONLYITEM, self.SRIList)  # 对人工确认唯一编码进行统一处理
                    eleMUI.vector = getVectorString(uniform)  # 新增：更新向量
                    eleMUI.status = 0  # 将状态更新成0，表示已经成为历史物料
                    # 添加数据到redis和newList中
                    dicts = dict(eleMUI)
                    self.newList.append(self._jsonStringToMMU(dicts))  # 目的：将更改后的数据添加到等待同步到DB数据库的列表中
                    self._writeToRedis(uniform, eleMUI)
                elif eleMUI.status == 2:  # 当状态为2时，说明该图号已经被解绑，也需要对其进行一次更新向量的操作
                    eleMUI.vector = getVectorString(eleMUI.uniformitem)  # 删除：更新向量
                    eleMUI.status = 0
                    dicts = dict(eleMUI)
                    self.newList.append(self._jsonStringToMMU(dicts))  # 目的：将更改后的数据添加到等待同步到DB数据库的列表中
                    self._writeToRedis(eleMUI.uniformitem, eleMUI)
                else:
                    self._writeToRedis(eleMUI.uniformitem, eleMUI)

        if self.newList:  # 如果存在新增或删除也要同时更新DB数据库
            self.conn_db.upsertBatchRow(self.newList)

        return self.MUIListIsDigit, self.MUIListNoDigit
