# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Adam Solchenberger <asolchenberger@gmail.com>
# Copyright (c) 2022 Jason Engman <engmanj@gmail.com>
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
from .connection import Connection

__all__ = ["DataManager"]

class DataManager(APIClass):

    def __repr__(self) -> str:
        return "<class> DataManager"
        
    def __init__(self) -> None:
        super().__init__()
        self.properties += ['db', 'db_connection', 'connections']
        self._db = None
        self._connections = {}
        print(self.properties)

    @property
    def db(self) -> bool:
        return bool(self._db) #True if loaded, False if unset

    @db.setter
    def db(self, path:str = None) -> bool:
        return self.load_database(path)

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
        id = max(self.connections) + 1 if len(self.connections) else 1
        self.connections[id] = Connection(params)
        return self.connections[id]

    def new_connection_from_db(self, id) -> None or "Connection":
        """
        query the active db file for the connection. If exists,
        load instantiate one, add it to self._connections and
        read params from db. If no db loaded or id not in it
        return None, else return the Connection() 
        """
    def load_db(self, path: str) -> bool:
        """
        load the settings db. return True if successful, else false
        if db is already loaded, this should close down the existing one first.
        this method only loads the db to the manager and gets the session
        ready. User is to call loading connections, tags, etc.
        """
        return False
    def query_for_connections(self) -> list:
        """
        if db open, query it for a list of the connection ids. if not open or none
        in it return []
        """
        return []