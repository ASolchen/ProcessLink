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
from typing import Any, Optional, Callable
import os

from pycomm3 import tag
from .api import APIClass, PropertyError

from .connection import Connection, TAG_TYPES
from .tag import Tag
from .database import ConnectionDb, DatabaseError
from .subscription import SubscriptionDb
__all__ = ["DataManager"]

CONNECTION_TYPES = {'local': Connection}
try:
    from .connections.logix import LogixConnection, LogixTag
    CONNECTION_TYPES['logix'] =  LogixConnection
    TAG_TYPES['logix'] =  LogixTag
except ImportError:
    pass

class DataManager(APIClass):

    def __repr__(self) -> str:
        return "<class> DataManager"
        
    def __init__(self) -> None:
        super().__init__()
        self._connection_types = CONNECTION_TYPES
        self._tag_types = TAG_TYPES
        self.properties += ['db_file', 'db_connection', 'connections', 'connection_types']
        self._db = None
        self._connections = {}
        self._db_file = None
        self.db_interface = ConnectionDb()
        self.sub_db = SubscriptionDb()
        self.sub_callbacks = {}


    @property
    def connection_types(self) -> dict:
        return [key for key in self._connection_types]

    @property
    def db_file(self) -> str or None:
        return self._db_file

    @db_file.setter
    def db_file(self, path:str) -> None:
        self._db_file = path

    @property
    def db_connection(self) -> list:
        return self.query_for_connections()

    @property
    def connections(self):
        return self._connections

    def new_connection(self, params) -> "Connection":
        """
        pass params for the properties of the connection. This will include
        the connection type and extended properties for that type
        return the Connection() 
        """
        try:
            params['connection_type']
        except KeyError as e:
            raise PropertyError(f'Error creating connection, missing parameter: {e}')
        try:
            self.connections[params["id"]] = CONNECTION_TYPES[params['connection_type']](params)
            return self.connections[params["id"]]
        except KeyError as e:
            raise PropertyError(f'Error creating connection, unknown type: {e}')

    def parse_tagname(self, tagname: str) -> list[str, str]:
        return tagname.replace("[","").split("]")

    def subscribe(self, tagname: str, id: str, callback: Callable) -> "Tag":
        """
        subscribe to a tag. Expected tagname format is '[connection]tag'
        sends the callback a tag update in a dictionary e.g.
        {"[MyConn]MyTag}": {"value": 3.14, "timestamp": 16000203.7}, }.
        If multiple subscriptions requested using the same id,
        the callback is sent multiple tags a the same time.
        """
        conn_name, tag_name = self.parse_tagname(tagname)
        tag = None
        conn = self.connections.get(conn_name)
        if conn:
            tag = conn.tags.get(tag_name)
        if conn and tag:
            sub = self.sub_db.session.query(self.sub_db.orm)\
                .filter(self.sub_db.orm.sub_id == id)\
                .filter(self.sub_db.orm.connection == conn_name)\
                .filter(self.sub_db.orm.tag == tag_name)\
                    .first()
            if sub == None:
                sub = self.sub_db.orm()
                sub.sub_id = id
                sub.connection = conn_name
                sub.tag = tag_name
                self.sub_db.session.add(sub)
                self.sub_db.session.commit()
            return True # found
        return False

    def load_db(self) -> bool:
        """
        load the settings db. return True if successful, else false
        if db is already loaded, this should close down the existing one first.
        this method only loads the db to the manager and gets the session
        ready. User is to call loading connections, tags, etc.
        """
        self.db_interface.db_file = self._db_file
        self.db_interface.open()
        session = self.db_interface.session
        orm = ConnectionDb.models["connection-params-local"]
        conns = session.query(orm).all()
        for conn in conns:
            params = CONNECTION_TYPES[conn.connection_type].get_params_from_db(session, conn.id)
            conn_obj = self.new_connection(params)
            conn_obj.load_tags_from_db(session)
        return True

    def close_db(self) -> bool:
        """
        close the settings db. return None
        """
        self.db_interface.close()
    
    def save_connection(self, conn: "Connection") -> None:
        if self.db_interface.session:
            conn.save_to_db(self.db_interface.session)
    
    def save_tag(self, tag: "Tag") -> None:
        if self.db_interface.session:
            tag.save_to_db(self.db_interface.session)