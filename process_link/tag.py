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
from .api import APIClass, PropertyError
from .subscription import SubscriptionDb
__all__ = ["Tag"]

class Tag(APIClass):
    """
    The base tag class
    """
    orm = SubscriptionDb.models['tag-params-local']

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
    
    @property
    def tagname(self) -> str:
        return f"[{self.connection_id}]{self.id}"


    @classmethod
    def add_to_db(cls, plink, params):
        plink.add_query(lambda session: session.add(Tag.orm(id=params['id'],
                                            connection_id=params['connection_id'],
                                            description=params.get('description', ''),
                                            datatype=params.get('datatype', ''),
                                            tag_type=params.get('tag_type', ''),
                                            value=params.get('value', ''),
                                            )))


    @classmethod
    def get_def_from_db(cls, plink, id):
        params = None        
        query = lambda session: session.query(Tag.orm).filter(Tag.orm.id == id).limit(1).all()
        res = plink.add_query(query, cols=['id', 'connection_id', 'description', 'datatype', 'tag_type'])
        if res:
            params = res[0]
            c_type = params.get('connection_type')
            if c_type in plink.c_types and not c_type == "local": #get the extended params
                params.update(plink.c_types[params.get('connection_type')].get_def_from_db(plink, id))
        return params

    def __repr__(self) -> str:
        return f"Tag: [{self.connection_id}]{self.id}"
        
    def __init__(self, params: dict) -> None:
        super().__init__()
        self.properties += ['tagname', 'id', 'connection_id', 'datatype', 'description', 'value']
        try:
            params['id']
            params['connection_id']
        except KeyError as e:
            raise PropertyError(f"Missing expected property {e}")
        self._id = params.get("id")
        self._tag_type = "local" #1=base tag. Override this on exetended class' init to the correct type
        self._datatype = params.get("datatype")
        self._description = params.get("description")
        self._value = params.get("value")
        self._connection_id = params["connection_id"]
        self.base_orm = SubscriptionDb.models['tag-params-local']
    
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
            self._id = entry.id
        return entry.id

########################New
    def delete_from_db(self,session: "db_session",tag_id):
        if tag_id != None:
            session.query(self.base_orm).filter(self.base_orm.id == tag_id).delete()
            session.commit()
########################New
