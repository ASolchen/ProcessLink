from typing import Any, Optional
import os, time

from sqlalchemy.sql.expression import table
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base, relationship, backref
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql.sqltypes import BIGINT, Float, Numeric

DeclarativeBase = declarative_base()

class ConnectionTable(DeclarativeBase):
    __tablename__ = 'connection-params-local'
    id = Column(String, primary_key=True)
    connection_type = Column(String, nullable=False)
    description = Column(String)


class ConnectionParamsModbusRTU(DeclarativeBase):
    __tablename__= 'connection-params-modbusRTU'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    port = Column(String)
    station_id = Column(String, default=1)
    baudrate = Column(Integer, default=9600)
    timeout = Column(Float, default=3.0)
    stop_bit = Column(Integer, default=1)
    parity = Column(String, default='N')
    byte_size = Column(Integer, default=8)
    retries = Column(Integer, default=3)
    

class ConnectionParamsModbusTCP(DeclarativeBase):
    __tablename__= 'connection-params-modbusTCP'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    host = Column(String, default='127.0.0.1')
    port = Column(Integer, default=502)
    station_id = Column(String, default=1)


class ConnectionParamsEthernetIP(DeclarativeBase):
    __tablename__= 'connection-params-logix'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    host = Column(String, default='127.0.0.1') #uses pycomm3 syntax for PLC path
    port = Column(Integer, default=44818)

class ConnectionParamsOPC(DeclarativeBase):
    __tablename__= 'connection-params-opc'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    host = Column(String, default='opc.tcp://127.0.0.1:49320') #uses pyopc url syntax for path

class ConnectionParamsGrbl(DeclarativeBase):
    __tablename__= 'connection-params-grbl'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    port = Column(String, default='/dev/ttyACM0')

class TagTable(DeclarativeBase): # this table holds all tag values being subscribed to
    __tablename__ = 'tag-params-local'
    id = Column(String, primary_key=True) #tag unique id is a combo of tag and connection ids
    connection_id = Column(String, ForeignKey(ConnectionTable.id), primary_key=True)
    tag_type = Column(String, nullable=False)
    description = Column(String)
    datatype = Column(String)
    value = Column(String) # used for retenitive tags


class TagParamsEthernetIP(DeclarativeBase):
    __tablename__= 'tag-params-logix'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, primary_key=True)
    connection_id = Column(String, primary_key=True)
    __table_args__ = (ForeignKeyConstraint([id, connection_id],
                                           [TagTable.id, TagTable.connection_id], ondelete='CASCADE'),
                      {})
    address = Column(String, nullable=False)


class TagParamsModbus(DeclarativeBase):
    __tablename__= 'tag-params-modbus'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, primary_key=True)
    connection_id = Column(String, primary_key=True)
    __table_args__ = (ForeignKeyConstraint([id, connection_id],
                                           [TagTable.id, TagTable.connection_id], ondelete='CASCADE'),
                      {})
    address = Column(Integer, nullable=False)
    bit = Column(Integer, default=0)
    word_swapped = Column(Boolean, default=False)
    byte_swapped = Column(Boolean, default=False)


class TagParamsOPC(DeclarativeBase):
    __tablename__= 'tag-params-opc'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, primary_key=True)
    connection_id = Column(String, primary_key=True)
    __table_args__ = (ForeignKeyConstraint([id, connection_id],
                                           [TagTable.id, TagTable.connection_id], ondelete='CASCADE'),
                      {})
    node_id = Column(String, nullable=False)

class TagParamsGrbl(DeclarativeBase):
    __tablename__= 'tag-params-grbl'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, primary_key=True)
    connection_id = Column(String, primary_key=True)
    __table_args__ = (ForeignKeyConstraint([id, connection_id],
                                           [TagTable.id, TagTable.connection_id], ondelete='CASCADE'),
                      {})
    address = Column(String, nullable=False)

class SubscriptionTable(DeclarativeBase): # this table holds all tag values being subscribed to
    __tablename__ = 'subscriptions'
    id = Column(Integer, primary_key=True)
    sub_id = Column(String, nullable=False)
    connection = Column(String, nullable=False)
    tag = Column(String, nullable=False)
    last_read = Column(Float, default=0.0)
    latest_only = Column(Boolean) #if True, latest value overwrites else all are buffered between updates

class DataTable(DeclarativeBase):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    tagname = Column(String, nullable=False)
    value = Column(String)
    timestamp = Column(Float)

class SubscriptionDb(object):
    models = {
        "data-table": DataTable,
        "subscription-table": SubscriptionTable,
        "connection-params-local": ConnectionTable,
        "connection-params-logix":    ConnectionParamsEthernetIP,
        "connection-params-modbusRTU": ConnectionParamsModbusRTU,
        "connection-params-modbusTCP": ConnectionParamsModbusTCP,
        "connection-params-opc": ConnectionParamsOPC,
        "connection-params-grbl": ConnectionParamsGrbl,
        "tag-params-local":  TagTable,
        "tag-params-logix":    TagParamsEthernetIP,
        "tag-params-modbus": TagParamsModbus,
        "tag-params-opc": TagParamsOPC,
        "tag-params-grbl": TagParamsGrbl
        }

    def __init__(self) -> None:
        self.sub_orm = SubscriptionTable
        self.data_orm = DataTable
        self.engine = create_engine('sqlite://')
        DeclarativeBase.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()



    def run_query(self, query, cols):
        
        res =  query(self.session)
        self.session.commit()
        #if no columns, the query doesn't need to return rows, e.g. deletes or updates
        if res and len(cols):
            new_res = []
            for r in res:
                row_dict = {}
                for col in cols:
                    row_dict[col]=r.__getattribute__(col)
                new_res.append(row_dict)
            res = new_res
        return res
    

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
