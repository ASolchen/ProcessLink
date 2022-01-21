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
from .tag import Tag
from .database import ConnectionDb

__all__ = ["Connection"]

class Connection(APIClass):
    """
    The base connection class
    """

    def __repr__(self) -> str:
        return "<class> Connection"

    @property
    def id(self) -> str:
        return self._id

    @property
    def connection_type(self) -> int:
        return self._connection_type

    @property
    def description(self):
        return self._description
    @description.setter
    def description(self, value: str) -> None:
        self._description = value


    @property
    def tags(self):
        return self._tags

    def __init__(self, params: dict) -> None:
        super().__init__()
        self.properties += ['id', 'connection_type', 'description', 'tags']
        self._id = params.get('id')
        self._connection_type = 1 #1=base connection. Override this on exetended class' init to the correct type
        self._description = params.get('description')
        self._tags = {}
        self.base_orm = ConnectionDb.models["connections"] # database object-relational-model
        #then set props
    
    def new_tag(self, params) -> "Tag":
        """
        pass params for the properties of the tag. This will include
        the connection type and extended properties for that type
        return the Tag() 
        """
        self.tags[params["id"]] = Tag(params)
        return self.tags[params["id"]]

    def save_to_db(self, session: "db_session") -> int:
        entry = session.query(self.base_orm).filter(self.base_orm.id == self.id).first()
        if entry == None:
            entry = self.base_orm()
        entry.id = self.id
        entry.connection_type = self.connection_type
        entry.description = self.description
        session.add(entry)
        session.commit()
        if not self._id == entry.id:
            self._id = entry.id # if db created this, the widget has a new id
        return entry.id