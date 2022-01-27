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

SubscriptionBase = declarative_base()

class SubscriptionTable(SubscriptionBase): # this table holds all tag values being subscribed to
    __tablename__ = 'subscriptions'
    id = Column(Integer, primary_key=True)
    sub_id = Column(String, nullable=False)
    connection = Column(String, nullable=False)
    tag = Column(String, nullable=False)
    value = Column(String)
    timestamp = Column(Float)
    error = Column(String)


class SubscriptionDb(object):
    def __init__(self) -> None:
        self.orm = SubscriptionTable
        self.engine = create_engine('sqlite://')
        SubscriptionBase.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()