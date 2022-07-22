
from ZHJYipinX.common.CollectFunc import mkList
from ZHJYipinX.bean.BatchMaterialUniformInfo import BatchMaterialUniform
from ZHJYipinX.bean.MainMaterialUniformInfo import MainMaterialUniform
from ZHJYipinX.contants.Variables import getVariable
from ZHJYipinX.process.Similarity import similarityNp, cosinesimilarityNp
from ZHJYipinX.util.LogUtil import MyLog

mylog = MyLog()


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
    vecInt2 = mkList(elemBUI.vector)
    vecInt1 = mkList(elemMUI.vector)
    sim: float = 0.0
    if len(elemBUI.vector) == 15 and len(elemBUI.vector) == len(elemMUI.vector):
        sim = similarityNp(vecInt1=vecInt1, vecInt2=vecInt2)  # 平均相似度
    else:
        sim = cosinesimilarityNp(vecInt2, vecInt1)  # 余弦相似度

    if sim >= similarthreshold:

        return getVariable(mbr)(elemBUI.soid, elemBUI.ITEM_ID, elemMUI.ITEM_ID, sim, elemMUI.ITEM_NAME_CH,
                                elemMUI.ITEM_NAME_EN, elemMUI.ITEM_UNIT, elemMUI.PDM_ID, elemMUI.ONE_TYPE, 1,
                                elemMUI.PEOPLE_ONLYITEM, elemMUI.PEOPLE_NAME_CH, elemMUI.PEOPLE_NAME_EN,
                                elemMUI.PEOPLE_UNIT)
    else:
        return 0
