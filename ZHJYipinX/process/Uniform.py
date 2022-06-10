
from typing import List
from multiprocessing import Pool
from ZHJYipinX.common.CalculateVector import getVectorString
from ZHJYipinX.common.CalculateSymbol import getNoSymbol
from ZHJYipinX.bean.BatchMaterialInfo import BatchMaterial
from ZHJYipinX.bean.MainMaterialInfo import MainMaterial
from ZHJYipinX.bean.BatchMaterialUniformInfo import BatchMaterialUniform
from ZHJYipinX.bean.MainMaterialUniformInfo import MainMaterialUniform
from ZHJYipinX.bean.SpecialCharactersInfo import StrReplaceInfo

def StrUniform(byUniformDataInfo, elemList: List[StrReplaceInfo]):
    """
    单进程统一字符串编码和向量化
    :param byUniformDataInfo: 是一个被统一的DataInfo实例对象
    :param elemList: 用于需要统一的元素对象
    :param UniformDataInfo: 结果输出封装对象
    :return: 返回统一后的数据对象
    """
    # print('<进程%s> StrUniform get  %s' % (os.getpid(), byUniformDataInfo.item_id))
    # TODO 1- 计算统一码
    newstr6 = getNoSymbol(byUniformDataInfo.ITEM_ID, elemList)
    # TODO 2- 计算向量 CalculateVector.getVectorString
    newstr7 = getVectorString(newstr6)
    # TODO 3- 封装成实例对象 MaterialUniformInfo
    # 加一个byUniformDataInfo的类型判断
    if isinstance(byUniformDataInfo, BatchMaterial) == True:
        return BatchMaterialUniform(byUniformDataInfo.soid, byUniformDataInfo.ITEM_ID, newstr6,
                                    byUniformDataInfo.ITEM_NAME_EN, byUniformDataInfo.ITEM_NAME_CH,
                                    byUniformDataInfo.ITEM_UNIT, newstr7)

    elif isinstance(byUniformDataInfo, MainMaterial) == True:

        return MainMaterialUniform(byUniformDataInfo.PDM_ID, byUniformDataInfo.ITEM_ID, newstr6,
                                   byUniformDataInfo.ITEM_NAME_EN, byUniformDataInfo.ITEM_NAME_CH,
                                   byUniformDataInfo.ITEM_UNIT, byUniformDataInfo.ONE_TYPE, newstr7, 0,
                                   byUniformDataInfo.ITEM_ID_ORIGINAL, byUniformDataInfo.PEOPLE_ONLYITEM,
                                   byUniformDataInfo.PEOPLE_NAME_CH, byUniformDataInfo.PEOPLE_NAME_EN,
                                   byUniformDataInfo.PEOPLE_UNIT)  # 状态为0表示已有主数据

    else:
        raise Exception("在StrUniform方法中，期待传入参数byUniformDataInfo的类型为BatchMaterial或MainMaterial")


def multProcessUniform(core_process, MIList, sriList):
    """
    多进程统一字符串编码和向量化
    :param core_process:
    :param MIList:
    :param sriList:
    :return:
    """
    p = Pool(core_process)
    MUIListR = []
    for elemBUI in MIList:
        if elemBUI != None:
            res = p.apply_async(StrUniform, args=(elemBUI, sriList,))
            MUIListR.append(res)
    p.close()
    p.join()  # 等待进程池中所有进程执行完毕

    MUIList = []
    for ele in MUIListR:
        MUIList.append(ele.get())

    return MUIList
