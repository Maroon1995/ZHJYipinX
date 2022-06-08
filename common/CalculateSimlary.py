from numpy import dot
from numpy.linalg import norm
from typing import List
from common.CollectFunc import mkList
from bean.BatchMaterialUniformInfo import BatchMaterialUniform
from bean.MainMaterialUniformInfo import MainMaterialUniform
from contants.Variables import getVariable
from util.LogUtil import MyLog

mylog = MyLog()


def cosinesimilarityNp(vecInt1: List[int], vecInt2: List[int]):
    """
    计算两个向量的余弦相似度
    :param vecInt1: 向量1
    :param vecInt2: 向量2
    :return: 返回相似度
    """
    res: float = 0.0
    if len(vecInt1) > 0 and len(vecInt1) == len(vecInt2):
        res = round(dot(vecInt1, vecInt2) / (norm(vecInt1) * norm(vecInt2)), 3)  # 计算相似度，并保留3位小数

    return res


def similar(elemBUI: BatchMaterialUniform, elemMUI: MainMaterialUniform, similarthreshold: float,
            mbr: str = "MaterialBatchResult"):
    """
    计算两个物料的相似度
    :param elemBUI: 物料数据信息（BatchMaterialUniform）
    :param elemMUI: 基础物料数据信息（MainMaterialUniform）
    :param similarthreshold: 相似度阈值
    :param mbr: 结果输出类型
    :return: 返回 MaterialBatchResult 和 0
    """
    vecInt1 = mkList(elemBUI.vector)
    vecInt2 = mkList(elemMUI.vector)

    sim = cosinesimilarityNp(vecInt1, vecInt2)

    if sim >= similarthreshold:

        return getVariable(mbr)(elemBUI.soid, elemBUI.ITEM_ID, elemMUI.ITEM_ID, sim, elemMUI.ITEM_NAME_CH,
                                elemMUI.PEOPLE_NAME_EN, elemMUI.ITEM_UNIT, elemMUI.PDM_ID, elemMUI.ONE_TYPE, 1,
                                elemMUI.PEOPLE_ONLYITEM, elemMUI.PEOPLE_NAME_CH, elemMUI.PEOPLE_NAME_EN,
                                elemMUI.PEOPLE_UNIT)
    else:
        return 0
