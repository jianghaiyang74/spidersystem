from sqlalchemy import create_engine,Column,Integer,String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from . import BaseFilter

Base = declarative_base()


# class Filter(Base):
#
#     __tablename__ = "filter"
#
#     id = Column(Integer,primary_key=True)
#     hash_value = Column(String(40),index=True,unique=True)

class MysqlFilter(BaseFilter):
    '''基于mysql去重判断'''
    def __init__(self,*args,**kwargs):

        self.table = type(
            kwargs["mysql_table_name"],
            (Base,),
            dict(
                __tablename__=kwargs["mysql_table_name"],
                id = Column(Integer, primary_key=True),
                hash_value = Column(String(40), index=True, unique=True)
            )
        )

        BaseFilter.__init__(self,*args,**kwargs)

    def _get_storage(self):
        '''返回一个mysql链接对象'''
        engine = create_engine(self.mysql_url)
        Base.metadata.create_all(engine) #创建表 如果有就忽略
        Session = sessionmaker(engine)
        return Session

    def _save(self,hash_value):
        '''利用set进行存储'''
        session = self.storage()
        filter = self.table(hash_value=hash_value)
        session.add(filter)
        session.commit()
        session.close()


    def _is_exists(self,hash_value):
        session = self.storage()
        ret = session.query(self.table).filter_by(hash_value=hash_value).first()
        session.close()
        if ret is None:
            return False
        return True
