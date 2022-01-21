# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Adam Solchenberger <asolchenberger@gmail.com>
# Copyright (c) 2022 Jason Engman <jengman@testtech-solutions.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

from typing import Any, Optional
import os

from sqlalchemy.sql.expression import table
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base, relationship, backref
from sqlalchemy import ForeignKey
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql.sqltypes import BIGINT, Float, Numeric

__all__ =["DatabaseError"]

class DatabaseError(Exception):
    """
    raised on database errors
    """

class DuplicateIdError(DatabaseError):
    """
    raised on attempt to create a dulpicate connection or tag
    """

ConnectionsBase = declarative_base()

class ConnectionTable(ConnectionsBase): # this table holds all tag values being subscribed to
    __tablename__ = 'connections'
    id = Column(String, primary_key=True)
    connection_type = Column(Integer, nullable=False)
    description = Column(String)


class ConnectionParamsModbusRTU(ConnectionsBase):
    __tablename__= 'connection-params-modbusRTU'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    status = Column(Integer) #what is this?
    port = Column(String)
    station_id = Column(String, default=1)
    baudrate = Column(Integer, default=9600)
    timeout = Column(Float, default=3.0)
    stop_bit = Column(Integer, default=1)
    parity = Column(String, default='N')
    byte_size = Column(Integer, default=8)
    retries = Column(Integer, default=3)
    


class ConnectionParamsModbusTCP(ConnectionsBase):
    __tablename__= 'connection-params-modbusTCP'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    status = Column(Integer) #what is this?
    host = Column(String, default='127.0.0.1')
    port = Column(Integer, default=502)
    station_id = Column(String, default=1)


class ConnectionParamsEthernetIP(ConnectionsBase):
    __tablename__= 'connection-params-ethernetIP'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    status = Column(Integer) #what is this?
    host = Column(String, default='127.0.0.1') #uses pycomm3 syntax for PLC path
    port = Column(Integer, default=44818)

class ConnectionParamsOPC(ConnectionsBase):
    __tablename__= 'connection-params-opc'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    status = Column(Integer) #what is this?
    host = Column(String, default='opc.tcp://127.0.0.1:49320') #uses pyopc url syntax for path

class ConnectionParamsGrbl(ConnectionsBase):
    __tablename__= 'connection-params-grbl'
    relationship('ConnectionTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(ConnectionTable.id, ondelete='CASCADE'), primary_key=True)
    pollrate = Column(Float, default=0.5)
    auto_connect = Column(Boolean, default=False)
    status = Column(Integer) #what is this?
    port = Column(String, default='/dev/ttyACM0')

class TagTable(ConnectionsBase): # this table holds all tag values being subscribed to
    __tablename__ = 'tags'
    id = Column(String, primary_key=True) #tag unique id is a combo of tag and connection ids
    connection_id = Column(String, ForeignKey(ConnectionTable.id), primary_key=True)
    tag_type = Column(Integer, nullable=False)
    description = Column(String)
    datatype = Column(String)
    value = Column(String) # used for retenitive tags

class TagParamsLocal(ConnectionsBase):
    __tablename__= 'tag-params-local'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(TagTable.id, ondelete='CASCADE'), primary_key=True)
    address = Column(String, nullable=False)

class TagParamsEthernetIP(ConnectionsBase):
    __tablename__= 'tag-params-ethernetIP'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(TagTable.id, ondelete='CASCADE'), primary_key=True)
    address = Column(String, nullable=False)

class TagParamsModbus(ConnectionsBase):
    __tablename__= 'tag-params-modbus'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(TagTable.id, ondelete='CASCADE'), primary_key=True)
    address = Column(Integer, nullable=False)
    bit = Column(Integer, default=0)
    word_swapped = Column(Boolean, default=False)
    byte_swapped = Column(Boolean, default=False)

class TagParamsOPC(ConnectionsBase):
    __tablename__= 'tag-params-opc'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(TagTable.id, ondelete='CASCADE'), primary_key=True)
    node_id = Column(String, nullable=False)

class TagParamsGrbl(ConnectionsBase):
    __tablename__= 'tag-params-grbl'
    relationship('TagTable', backref=backref('children', passive_deletes=True))
    id = Column(String, ForeignKey(TagTable.id, ondelete='CASCADE'), primary_key=True)
    address = Column(String, nullable=False)

class ConnectionDb():
    models = {
        "connections": ConnectionTable,
        "connection-params-ethernetIP":    ConnectionParamsEthernetIP,
        "connection-params-modbusRTU": ConnectionParamsModbusRTU,
        "connection-params-modbusTCP": ConnectionParamsModbusTCP,
        "connection-params-opc": ConnectionParamsOPC,
        "connection-params-grbl": ConnectionParamsGrbl,
        "tags": TagTable,
        "tag-params-local":    TagParamsLocal,
        "tag-params-ethernetIP":    TagParamsEthernetIP,
        "tag-params-modbus": TagParamsModbus,
        "tag-params-opc": TagParamsOPC,
        "tag-params-grbl": TagParamsGrbl
        }

    def __init__(self) -> None:
        self.db_file = None
        self.session = None
        self.engine = None

    def open(self) -> None:
        if self.session:
            self.open()
        if not self.db_file:
            raise DatabaseError("Connection database attempt to open with no file path")
        self.engine = create_engine(f"sqlite:///{self.db_file}")
        ConnectionsBase.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    
    def close(self, *args: Any) -> None:
        if self.session:
            self.session.close()
        self.session = None
        self.engine = None