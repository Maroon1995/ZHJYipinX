from typing import Dict

from ZHJYipinX.bean.MaterialBatchResultInfo import MaterialBatchResult
from ZHJYipinX.bean.MaterialSingleResultInfo import MaterialSingleResult

di: Dict = {"MaterialBatchResult": MaterialBatchResult, "MaterialSingleResult": MaterialSingleResult}

def getVariable(mbr:str):
    """
    获取结果输出类型
    :param mbr:
    :return:
    """
    return di.get(mbr)