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

import re
from typing import Any, Optional
from .api import APIClass, PropertyError
from .tag import Tag
from .database import ConnectionDb

__all__ = ["Connection"]

TAG_TYPES = {'local': Tag}


class Connection(APIClass):
    """
    The base connection class
    """

    def __repr__(self) -> str:
        return f"Connection: {self.id}"

    @property
    def id(self) -> str:
        return self._id

    @property
    def connection_type(self) -> str:
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

    @classmethod
    def get_params_from_db(cls, session, id: str):
        params = None
        orm = ConnectionDb.models["connection-params-local"]
        conn = session.query(orm).filter(orm.id == id).first()
        if conn:
            params = {
                'id': conn.id,
                'connection_type': conn.connection_type,
                'description': conn.description,
            }
        return params

    

    def __init__(self, params: dict) -> None:
        super().__init__()
        self._tag_types = TAG_TYPES
        self.properties += ['id', 'connection_type', 'description', 'tags']
        try:
            params['id']
            params['connection_type']
        except KeyError as e:
            raise PropertyError(f"Missing expected property {e}")
        self._id = params.get('id')
        self._connection_type = "local" #base connection. Override this on exetended class' init to the correct type
        self._description = params.get('description')
        self._tags = {}
        self.base_orm = ConnectionDb.models["connection-params-local"] # database object-relational-model
        #then set props
    
    def new_tag(self, params) -> "Tag":
        """
        pass params for the properties of the tag. This will include
        the connection type and extended properties for that type
        return the Tag() 
        """
        params['connection_id'] = self.id
        try:
            self.tags[params["id"]] = TAG_TYPES[self.connection_type](params)
            return self.tags[params["id"]]
        except KeyError as e:
            raise PropertyError(f'Error creating tag, unknown type: {e}')
        

    def save_to_db(self, session: "db_session") -> str:
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
    
    def load_tags_from_db(self, session):
        orm = ConnectionDb.models['tag-params-local']
        tags = session.query(orm).filter(orm.connection_id == self.id).all()
        for tag in tags:
            params = TAG_TYPES[self.connection_type].get_params_from_db(session, tag.id)
            self.new_tag(params)

