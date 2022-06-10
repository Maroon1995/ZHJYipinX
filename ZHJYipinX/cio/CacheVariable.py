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

    redisUtil: RedisHelper
    conn_db: SQLUtil
    MUIListIsDigit: List = []
    MUIListNoDigit: List = []
    newList = []  # 存放status=2，需要被更新的MainMaterialUniform
    input: DataIn

    def __init__(self, expireTime: int, ):
        self.expireTime = expireTime

    def _jsonStringToMMU(self,dicts: Dict):
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

    def getIsOrNoDigitList(self,username: str, redisUtil: RedisHelper, expireTime: int):
        """
        读取redis缓存数据，并转换成List[MaterialUniform]
        :param username: redis中的哈希表名称
        :param redisUtil: redis工具类
        :return:
        """

        MUIListDict = redisUtil.getall_values_hset(username, expireTime)

        return list(map(lambda x: self._jsonStringToMMU(json.loads(x)), MUIListDict))

    def _writeToRedis(self,field, eleMUI):

        new_data = json.dumps(dict(eleMUI))
        if eleMUI.uniformitem.isdigit():  # 过滤，分类出数值字符串和非数值字符串
            self.MUIListIsDigit.append(eleMUI)  # 添加到列表
            self.redisUtil.add_hset("MUIListIsDigit", eleMUI.ITEM_ID, new_data,
                               self.expireTime)  # 添加到缓存哈希表“MUIListIsDigit”中
        else:
            self.MUIListNoDigit.append(eleMUI)
            self.redisUtil.add_hset("MUIListNoDigit", eleMUI.ITEM_ID, new_data, self.expireTime)


    def CacheValue(self):
        """
        input: DataIn, SRIList: List[StrReplaceInfo]
            使其以服务的形式在后台一直执行
        :param input: 实例化的输出对象DataIn
        :param SRIList: 替换字符列表
        :param conn_db: DB数据库实例对象SQLUtil
        :param redisUtil: redis工具类实例
        :param expireTime: redis中数据有效期
        :return: MUIListIsDigit, MUIListNoDigit
        """
        filterCondition = or_(MainMaterialUniform.status == 1, MainMaterialUniform.status == 2)  # 状态条件，1表示新增，2表示删除
        # 查询MainMaterialUniform表中是否存在status为1的数据，若存在则更缓存redis
        flag: int = self.conn_db.mainKeyExists(MainMaterialUniform, filterCondition)
        if flag == 0 and self.redisUtil.is_existsKey("MUIListIsDigit", self.expireTime) == True and self.redisUtil.is_existsKey(
                "MUIListNoDigit", self.expireTime) == True:  # 如果，两个username如果均存在且flag=0，则从redis中获取
            print("从redis中获取")
            MUIListIsDigit = self.getIsOrNoDigitList("MUIListIsDigit", self.redisUtil, self.expireTime)
            MUIListNoDigit = self.getIsOrNoDigitList("MUIListNoDigit", self.redisUtil, self.expireTime)

        elif flag == 1 and self.redisUtil.is_existsKey("MUIListIsDigit", self.expireTime) == True and self.redisUtil.is_existsKey(
                "MUIListNoDigit", self.expireTime) == True:  # 如果，两个username均存在, 且flag=1，则更新redis中对应的值，然后再获取数据
            # 从DB中获取MainMaterialUniform中status == 1的所有数据
            MUIListStatus: List[MainMaterialUniform] = self.input.mysqlFileData(MainMaterialUniform,
                                                                           filterCondition=filterCondition)
            for i in range(len(MUIListStatus)):  # 更新数据
                eleMUIStatus = MUIListStatus[i]
                if eleMUIStatus.status == 1:
                    eleMUIStatus.vector = getVectorString(eleMUIStatus.PEOPLE_ONLYITEM)  # 新增：更新向量
                    # eleMUIStatus.uniformitem = getNoSymbol(eleMUIStatus.PEOPLE_ONLYITEM, SRIList)  # 更新uniformitem
                if eleMUIStatus.status == 2:
                    eleMUIStatus.vector = getVectorString(eleMUIStatus.uniformitem)  # 删除：更新向量
                eleMUIStatus.status = 0  # 将状态更新成0，表示已经成为历史物料
                new_data = json.dumps(dict(eleMUIStatus))
                dicts = dict(eleMUIStatus)
                self.newList.append(self._jsonStringToMMU(dicts))  # 将更后的数据添加到等待同步到DB数据库的列表中

                if self.redisUtil.isExists_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID,
                                           self.expireTime) == 1:  # 如果存在,则先删除该key-value，再添加key_newvalue
                    self.redisUtil.del_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID, self.expireTime)
                    self.redisUtil.add_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID, new_data, self.expireTime)

                elif self.redisUtil.isExists_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID,
                                             self.expireTime) == 1:  # 如果存在,则先删除该key-value，再添加key_newvalue
                    self.redisUtil.del_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID, self.expireTime)
                    self.redisUtil.add_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID, new_data, self.expireTime)
                else:
                    if eleMUIStatus.uniformitem.isdigit():  # 以上两种情况都不存在，则直接判断是否为数值型字符串，然后添加
                        self.redisUtil.add_hset("MUIListIsDigit", eleMUIStatus.ITEM_ID, new_data, self.expireTime)
                    else:
                        self.redisUtil.add_hset("MUIListNoDigit", eleMUIStatus.ITEM_ID, new_data, self.expireTime)

            print("从redis中获取")
            MUIListIsDigit = self.getIsOrNoDigitList("MUIListIsDigit", self.redisUtil, self.expireTime)
            MUIListNoDigit = self.getIsOrNoDigitList("MUIListNoDigit", self.redisUtil, self.expireTime)

        else:  # 否则，从DB数据库中获取,并重新更新redis中的全部数据
            # 从数据库中获取数据
            print("从DB数据库获取,并更新数据")
            # 从数据库中获取数据
            MUIList: List[MainMaterialUniform] = self.input.mysqlFileData(MainMaterialUniform)
            # 清除一次redis
            self.redisUtil.flushData("MUIListIsDigit")
            self.redisUtil.flushData("MUIListNoDigit")
            # ---------------------
            for i in range(len(MUIList)):
                eleMUI = MUIList[i]
                # -----------------------------------------------
                # （1）更新MainMaterialUniform中的向量vector，当状态status为1的时候
                if eleMUI.status == 1:  # 当eleMUI状态为1时，说明该图号已经被确认过，需要对其进行一次更新向量的操作
                    eleMUI.vector = getVectorString(eleMUI.PEOPLE_ONLYITEM)  # 新增：更新向量
                    eleMUI.status = 0  # 将状态更新成0，表示已经成为历史物料
                    dicts = dict(eleMUI)
                    self.newList.append(self._jsonStringToMMU(dicts))  # 将更后的数据添加到等待同步到DB数据库的列表中
                    # -----------------------------------------------
                    # (2) 更新缓存redis
                    new_data = json.dumps(dict(eleMUI))
                    if eleMUI.PEOPLE_ONLYITEM.isdigit():  # 过滤，分类出数值字符串和非数值字符串
                        self.MUIListIsDigit.append(eleMUI)  # 添加到列表
                        self.redisUtil.add_hset("MUIListIsDigit", eleMUI.ITEM_ID, new_data,
                                           self.expireTime)  # 添加到缓存哈希表“MUIListIsDigit”中
                    else:
                        self.MUIListNoDigit.append(eleMUI)
                        self.redisUtil.add_hset("MUIListNoDigit", eleMUI.ITEM_ID, new_data, self.expireTime)

                if eleMUI.status == 2:
                    eleMUI.vector = getVectorString(eleMUI.uniformitem)  # 删除：更新向量
                    # eleMUI.uniformitem = getNoSymbol(eleMUI.PEOPLE_ONLYITEM, SRIList)  # 更新uniformitem
                    eleMUI.status = 0  # 将状态更新成0，表示已经成为历史物料
                    dicts = dict(eleMUI)
                    self.newList.append(self._jsonStringToMMU(dicts))  # 将更后的数据添加到等待同步到DB数据库的列表中

                # -----------------------------------------------
                # (2) 更新缓存redis
                new_data = json.dumps(dict(eleMUI))
                if eleMUI.uniformitem.isdigit():  # 过滤，分类出数值字符串和非数值字符串
                    self.MUIListIsDigit.append(eleMUI)  # 添加到列表
                    self.redisUtil.add_hset("MUIListIsDigit", eleMUI.ITEM_ID, new_data,
                                       self.expireTime)  # 添加到缓存哈希表“MUIListIsDigit”中
                else:
                    self.MUIListNoDigit.append(eleMUI)
                    self.redisUtil.add_hset("MUIListNoDigit", eleMUI.ITEM_ID, new_data, self.expireTime)

        if self.newList:  # 如果存在新增或删除也要同时更新DB数据库
            self.conn_db.upsertBatchRow(self.newList)

        return self.MUIListIsDigit, self.MUIListNoDigit
