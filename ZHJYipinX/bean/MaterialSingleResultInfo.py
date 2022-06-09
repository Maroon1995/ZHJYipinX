# 导入定义类需要的模块
from sqlalchemy.ext.declarative import declarative_base  # 调用sqlalchemy的基类
from sqlalchemy import Column  # 指定字段属性，索引、唯一、DML
from sqlalchemy.types import *  # 所有字段类型
from ZHJYipinX.cio.Configuration import readConfig

# 创建库表类型
Base = declarative_base()  # 调用sqlalchemy的基类


class MaterialSingleResult(Base):
    '''
    数据统一后的数据对象
    '''
    __tablename__ = readConfig()["tablename"]["MaterialSingleResult"]  # 数据表的名字
    __table_args__ = {'extend_existing': True}  # 当数据库中已经有该表时，或内存中已声明该表，可以用此语句重新覆盖声明。
    oid = Column(Integer, primary_key=True)
    soid = Column(String(255), unique=True)
    ITEM_ID_ORIGINAL = Column(String(255), unique=True)
    ITEM_ID = Column(String(128), unique=True)
    SIMILAR = Column(Float, unique=True)
    ITEM_NAME_CH = Column(String(255), unique=True)
    ITEM_NAME_EN = Column(String(255), unique=True)
    ITEM_UNIT = Column(String(255), unique=True)
    PDM_ID = Column(String(255), unique=True)
    ONE_TYPE = Column(String(255), unique=True)
    MatchLogo = Column(Integer, unique=True)
    PEOPLE_ONLYITEM = Column(String(255), unique=True)
    PEOPLE_NAME_CH = Column(String(255), unique=True)
    PEOPLE_NAME_EN = Column(String(255), unique=True)
    PEOPLE_UNIT = Column(String(255), unique=True)

    def keys(self):
        return ["soid", "ITEM_ID_ORIGINAL", "ITEM_ID", "SIMILAR", "ITEM_NAME_CH", "ITEM_NAME_EN", "ITEM_UNIT", "PDM_ID",
                "ONE_TYPE", "MatchLogo", "PEOPLE_ONLYITEM", "PEOPLE_NAME_CH", "PEOPLE_NAME_EN", "PEOPLE_UNIT"]

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __init__(self, soid, ITEM_ID_ORIGINAL, ITEM_ID, SIMILAR, ITEM_NAME_CH, ITEM_NAME_EN, ITEM_UNIT, PDM_ID,
                 ONE_TYPE, MatchLogo, PEOPLE_ONLYITEM, PEOPLE_NAME_CH, PEOPLE_NAME_EN, PEOPLE_UNIT):
        self.soid = soid
        self.ITEM_ID_ORIGINAL = ITEM_ID_ORIGINAL
        self.ITEM_ID = ITEM_ID
        self.SIMILAR = SIMILAR
        self.ITEM_NAME_CH = ITEM_NAME_CH
        self.ITEM_NAME_EN = ITEM_NAME_EN  # 声明需要调用的特征，可以只声明数据库中表格列的子集
        self.ITEM_UNIT = ITEM_UNIT
        self.PDM_ID = PDM_ID
        self.ONE_TYPE = ONE_TYPE
        self.MatchLogo = MatchLogo
        self.PEOPLE_ONLYITEM = PEOPLE_ONLYITEM
        self.PEOPLE_NAME_CH = PEOPLE_NAME_CH
        self.PEOPLE_NAME_EN = PEOPLE_NAME_EN
        self.PEOPLE_UNIT = PEOPLE_UNIT
