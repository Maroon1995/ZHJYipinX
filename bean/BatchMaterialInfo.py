# 导入定义类需要的模块
from sqlalchemy.ext.declarative import declarative_base  # 调用sqlalchemy的基类
from sqlalchemy import Column, Index, distinct, update  # 指定字段属性，索引、唯一、DML
from sqlalchemy.types import *  # 所有字段类型
from cio.Configuration import readConfig

# 创建库表类型
Base = declarative_base()  # 调用sqlalchemy的基类

class BatchMaterial(Base):
    '''
    需要进行唯一化的批量数据
    '''
    __tablename__ = readConfig()["tablename"]["BatchMaterial"]  # 数据表的名字
    __table_args__ = {'extend_existing': True}
    oid = Column(Integer, primary_key=True)
    soid = Column(Integer, unique=True)
    ITEM_ID = Column(String(128), unique=True)
    ITEM_NAME_CH = Column(String(255), unique=True)
    ITEM_NAME_EN = Column(String(255), unique=True)
    ITEM_UNIT = Column(String(255), unique=True)

    def keys(self):
        return ["soid", "ITEM_ID", "ITEM_NAME_EN", "ITEM_NAME_CH", "ITEM_UNIT"]

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __init__(self, soid, ITEM_ID, ITEM_NAME_EN, ITEM_NAME_CH, ITEM_UNIT):
        self.soid = soid
        self.ITEM_ID = ITEM_ID
        self.ITEM_NAME_EN = ITEM_NAME_EN
        self.ITEM_NAME_CH = ITEM_NAME_CH
        self.ITEM_UNIT = ITEM_UNIT
