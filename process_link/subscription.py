from typing import Any, Optional
import os, time

from sqlalchemy.sql.expression import table
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base, relationship, backref
from sqlalchemy import ForeignKey
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql.sqltypes import BIGINT, Float, Numeric

SubscriptionBase = declarative_base()

class SubscriptionTable(SubscriptionBase): # this table holds all tag values being subscribed to
    __tablename__ = 'subscriptions'
    id = Column(Integer, primary_key=True)
    sub_id = Column(String, nullable=False)
    tagname = Column(String, nullable=False)
    last_read = Column(Float, default=0.0)
    latest_only = Column(Boolean, default=True) #if True, latest value overwrites else all are buffered between updates

class DataTable(SubscriptionBase):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    tagname = Column(String, nullable=False)
    value = Column(String)
    timestamp = Column(Float)

class SubscriptionDb(object):
    def __init__(self) -> None:
        self.sub_orm = SubscriptionTable
        self.data_orm = DataTable
        self.engine = create_engine('sqlite://')
        SubscriptionBase.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()



class UpdateHandler(object):
    """
    handles the passing of data between threads and mutex
    locking
    """
    def __init__(self) -> None:
        self.thread_lock = False
        self.tag_updates = {}  #tag updates stored by tagname. e.g
        # self.tag_updates = {
        #   "[Conx1]Tag01": [
        #          (3.14159, 1643503017.642834), # tuples of value, timestamp
        #          (3.14159, 1643503018.642834),
        #          (3.14159, 1643503019.642834),
        #          (3.14159, 1643503020.642834), 
        #           ],
        #   "[Conx1]Tag02": [
        #          (3.14159, 1643503017.642834),
        #          (3.14159, 1643503018.642834),
        #          (3.14159, 1643503019.642834),
        #          (3.14159, 1643503020.642834), 
        #           ],
        # }

    def aquire_lock(self)->None:
        while(self.thread_lock):
            time.sleep(0.001) #spin here until unlocked
        self.thread_lock = True #grab the lock
        return
    
    def remove_lock(self) -> None:
        self.thread_lock = False
    
    def store_updates(self, tag_updates: dict) -> None:
        """
        updates comming from multile threads (connections)
        """
        self.aquire_lock()
        for tag, updates in tag_updates.items():
            if not tag in self.tag_updates:
                self.tag_updates[tag] = []
            for update in updates:
                self.tag_updates[tag].append(update)
        self.remove_lock()

    def get_updates(self) -> dict:
        """
        called from the ProcessLink when
        the client code asks for updates
        """
        self.aquire_lock()
        updates = self.tag_updates.copy()
        self.tag_updates = {}
        self.remove_lock()
        return updates
