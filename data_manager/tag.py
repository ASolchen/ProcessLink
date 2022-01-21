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
from .api import APIClass
from .database import ConnectionDb
__all__ = ["Tag", "EthernetIpTag"]

class Tag(APIClass):
    """
    The base tag class
    """
    @property
    def id(self) -> str:
        return self._id

    @property
    def tag_type(self) -> int:
        return self._tag_type

    @property
    def connection_id(self) -> str:
        return self._connection_id

    @property
    def datatype(self) -> str:
        return self._datatype
    @datatype.setter
    def datatype(self, value: str) -> None:
        self._datatype = value

    @property
    def description(self) -> str:
        return self._description
    @description.setter
    def description(self, value: str) -> None:
        self._description = value
    

    @property
    def value(self) -> Any:
        return self._value
    @value.setter
    def value(self, value: str) -> None:
        self._value = value

    
    def __repr__(self) -> str:
        return f"Tag: [{self.connection_id}]{self.id}"
        
    def __init__(self, params: dict) -> None:
        super().__init__()
        self.properties += ['id', 'connection_id', 'datatype', 'description', 'value']
        self._id = params.get("id")
        self._tag_type = 1 #1=base tag. Override this on exetended class' init to the correct type
        self._datatype = params.get("datatype")
        self._description = params.get("description")
        self._value = params.get("value")
        self._connection_id = params["connection_id"]
        self.base_orm = ConnectionDb.models['tags']
    
    def save_to_db(self, session: "db_session") -> int:
        entry = session.query(self.base_orm).filter(self.base_orm.id == self.id).filter(self.base_orm.connection_id == self.connection_id).first()
        if entry == None:
            entry = self.base_orm()
        entry.id=self.id
        entry.tag_type=self.tag_type
        entry.connection_id=self.connection_id
        entry.description=self.description
        entry.datatype = self.datatype
        entry.value = self.value
        session.add(entry)
        session.commit()
        if not self._id == entry.id:
            self._id = entry.id # if db created this, the widget has a new id
        return entry.id

class EthernetIpTag(Tag):
    """
    The base tag class
    """

    def __repr__(self) -> str:
        return "<class> EthernetIP Tag"

    def __init__(self, params: dict) -> None:
        super().__init__(params)
        self._tags = {}
        self.properties += ['address', 'data_type', 'tags']