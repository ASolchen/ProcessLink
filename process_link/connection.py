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

import threading, time
import re
from typing import Any, Optional
from .api import APIClass, PropertyError
from .tag import Tag
from .data_types import DATA_TYPES
from .subscription import SubscriptionDb, SubscriptionTable, DataTable


__all__ = ["Connection", "TAG_TYPES", "UnknownConnectionError"]

TAG_TYPES = {'local': Tag}

class UnknownConnectionError(Exception):
    """
    raised when getting a connection that does not exist
    """


class Connection(APIClass):
    """
    The base connection class
    """
    data_types = DATA_TYPES
    orm = SubscriptionDb.models["connection-params-local"] # database object-relational-model
    tag_orm = SubscriptionDb.models['tag-params-local']

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
    
    @classmethod
    def add_to_db(cls, plink, params):
        plink.add_query(lambda session: session.add(Connection.orm(id=params['id'],
                                                                    connection_type=params['connection_type'],
                                                                    description=params.get('description', ''))))
    
    @classmethod
    def get_def_from_db(cls, plink, id):
        params = None        
        query = lambda session: session.query(Connection.orm).filter(Connection.orm.id == id).limit(1).all()
        res = plink.add_query(query, cols=['id', 'connection_type', 'description'])
        if res:
            params = res[0]
            c_type = params.get('connection_type')
            if c_type in plink.c_types and not c_type == "local": #get the extended params
                params.update(plink.c_types[params.get('connection_type')].get_def_from_db(plink, id))
        return params

    def __init__(self, process_link: "ProcessLink", params: dict) -> None:
        super().__init__()
        self.process_link = process_link
        self._tag_types = TAG_TYPES
        self.properties += ['id', 'connection_type', 'description', 'tags']
        try:
            params['id']
            params['connection_type']
        except KeyError as e:
            raise PropertyError(f"Missing expected property {e}")
        self._id = params.get('id')
        self._connection_type = "local" #base connection. Override this on exetended class' init to the correct type
        self._description = '' if 'description' not in params else params.get('description')
        self.tag_properties = ['id', 'connection_id', 'tag_type', 'description',
                               'datatype', 'value']
        self.polling = True
        self.poll_thread = threading.Thread(target=self.poll, daemon=True)
        self.update_tags_changed(True)
        self.poll_thread.start()

    def poll(self, *args):
        while(self.polling):
            ts = time.time()
            #does nothing on base for now
            #maybe add local tags to read and write to / from
            time.sleep((ts+0.5)-time.time())
    
    def return_tag_parameters(self,*args):
        #default for local connection
        return ['id', 'connection_id', 'description','datatype','tag_type']

    def get_sub_tags(self, param_list):
        """
        get_sub_tags() returns all distinct tag definitions for the connection to poll
        """
        sub_tags = {}
        res = self.process_link.add_query(lambda session: \
            session.query(SubscriptionTable.connection,
            SubscriptionTable.tag)\
            .filter(SubscriptionTable.connection == self._id).all(),
            cols=['connection', 'tag'])
        # need to query tag definitions for connections
        if res:
            tags = [t['tag']for t in res]
            for tag in tags:
                sub_tags[tag] = {}
                tag_res = self.process_link.add_query(lambda session: \
                    session.query(DataTable)\
                    .filter(DataTable.tagname == tag).all(),
                    cols=self.tag_properties)
                if tag_res:
                    sub_tags[tag].update(tag_res[0])

        return sub_tags

    def update_tags_changed(self, state):
        """
        updated the Connection table flag that tags may have changed
        """
        self.process_link.add_query(lambda session: session.query(Connection.orm)\
       .filter(Connection.orm.id == self._id)\
       .update({"polled_tags_changed": state})
        )
    
    def get_tags_changed(self):
        """
        get status of the Connection table flag that tags may have changed
        """
        res = self.process_link.add_query(lambda session: session.query(Connection.orm)\
            .filter(Connection.orm.id == self._id), cols=['polled_tags_changed']
        )
        state = None
        if len(res):
            state = res[0]['polled_tags_changed']
        return state




        

