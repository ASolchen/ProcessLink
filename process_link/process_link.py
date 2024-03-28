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
import threading
import queue
from typing import Any, Optional, Callable
import os, time
from sqlalchemy import asc, desc, and_
####################Different
from pycomm3 import tag
#####################
from .api import APIClass, PropertyError

from .connection import Connection, TAG_TYPES, UnknownConnectionError
from .tag import Tag
from .database import DatabaseError
from .subscription import SubscriptionTable, DataTable

from .subscription import SubscriptionDb
__all__ = ["ProcessLink"]

CONNECTION_TYPES = {'local': Connection}
from .connections.logix import LogixConnection, LogixTag
CONNECTION_TYPES['logix'] =  LogixConnection
TAG_TYPES['logix'] =  LogixTag
from .connections.modbus_tcp import ModbusTag, ModbusTCPConnection
CONNECTION_TYPES['modbusTCP'] =  ModbusTCPConnection
TAG_TYPES['modbusTCP'] =  ModbusTag


class ProcessLink(APIClass):
    c_types = CONNECTION_TYPES
    t_types = TAG_TYPES
    sub_table_orm = SubscriptionTable
    data_table_orm = DataTable

    def __repr__(self) -> str:
        return "<class> ProcessLink"
        
    def __init__(self) -> None:
        super().__init__()
        self.db = None
        self.query_queue = queue.Queue()
        self.result_queues = {}  # Dictionary to store result queues for each thread
        # Start query thread
        self.query_thread = threading.Thread(target=self._query_thread, daemon=True)
        self.query_thread.start()
        self.connection_thread = threading.Thread(target=self._connection_thread, daemon=True)
        self.connection_thread.start()
        self._connections = {} # dict for holding connection objects

    def _query_thread(self):
        """
        _query_thread() handles the queries in the queue, do not call outside of init
        :return: None
        """ 
        self.db = SubscriptionDb()
        while True:
            query_info, result_queue, query_ready_event = self.query_queue.get()
            query = query_info['query']
            cols = query_info['cols']

            # Execute the query within the session
            result = self.db.run_query(query, cols)

            # Put the result into the result queue
            result_queue.put(result)

            # Set the event to indicate that the query has been executed
            query_ready_event.set()

    def add_query(self, query, cols=[]):
        thread_id = threading.current_thread().ident  # Generate thread ID
        result_queue = queue.Queue()
        query_ready_event = threading.Event()

        # Add the result queue and event to the dictionary
        self.result_queues[thread_id] = (result_queue, query_ready_event)

        # Add the query information and related queue/event to the main queue
        query_info = {
            'query': query,
            'cols': cols
        }
        self.query_queue.put((query_info, result_queue, query_ready_event))

        # Wait for the query to be executed by the other thread
        query_ready_event.wait()

        # Get the result from the result queue associated with this thread
        result = result_queue.get()

        # Clear the event for the next query
        query_ready_event.clear()

        return result

    def _connection_thread(self):
        """
        _connection_thread() handles starting and stopping connections and cleaning up DataTable records that are not needed
        :return: None
        """ 
        LOOP_TM = 0.2 
        while(1):
            ts = time.time()
            #check for subs on connections we don't have open
            res = self.add_query(lambda session: session.query(SubscriptionTable.connection).distinct().all())
            sub_conx = []
            if res:
                sub_conx = [s[0] for s in res]
            # if needed, create a new connection from the db definintion
            for connection_id in sub_conx:
                if not connection_id in self._connections:
                    params = Connection.get_def_from_db(self, connection_id)
                    if params:
                        self._connections[connection_id] = self.c_types[params['connection_type']](self, params)
                    else:
                        UnknownConnectionError(connection_id)
            #clean up old tags
            #check for stale connections, close down and delete if not needed

            time.sleep(max(0, (ts+LOOP_TM)-time.time()))

    def new_connection(self, params):
        """
        pass params for the properties of the connection. This will include
        the connection type and extended properties for that type
        The connection definition is put in the database, and used to
        instantiate the Connection class when needed
        """
        try:
            params['connection_type']
        except KeyError as e:
            raise PropertyError(f'Error creating connection, missing parameter: {e}')
        try:
            self.c_types[params['connection_type']].add_to_db(self, params)
        except KeyError as e:
            raise PropertyError(f'Error creating connection, unknown type: {e}')
    
    def new_tag(self, params):
        """
        new_tag adds a tag definition using the params for the properties of the tag. This will include
        the connection type and extended properties for that type
        The tag definition is put in the database, and used by its
        Connection class when needed 
        """
        try:
            TAG_TYPES[params.get('tag_type')].add_to_db(self, params)
        except KeyError as e:
            raise PropertyError(f'Error creating tag, unknown type: {e}')
        
    def parse_tagname(self, tagname: str) -> list[str, str]:
        return tagname.replace("[","").split("]") #TODO <-fix this, will blow up if tag is an array like [plc]tagname[3]

    def subscribe(self, sub_id: str, tagname: str, latest_only: bool=True) -> "Tag":
        """
        subscribe to a tag. Expected tagname format is '[connection]tag'
        sends the updates back when requested later using get_tag_updates() e.g.
        {"[MyConn]MyTag}": {"value": 3.14, "timestamp": 16000203.7}, }.
        """ 
        connection, tag = self.parse_tagname(tagname)
        query = lambda session: session.add(SubscriptionTable(sub_id=sub_id,
                                                              connection=connection,
                                                              tag=tag,
                                                              latest_only=latest_only))
        self.add_query(query)
  
    def get_tag_updates(self, sub_id: str) -> None:
        """
        updates = {
            "[Fred0]Tag0": [(3.14159, 1600000000),(3.14159, 1600000001)] 
            "[Fred0]Tag1": [(4.14159, 1600000000),(4.14159, 1600000001)] 
            }
        all connections send tag updates to this method
        the tags in the update are check in the subscription
        table and grouped into subcription ids. The last
        callback is sent the update for those tags
        """
        ts = time.time() #use for subscription last_read
        #get all unique tags in the sub
        query = lambda session: session.query(SubscriptionTable).filter(SubscriptionTable.sub_id == sub_id).distinct().all()
        res = self.add_query(query, cols=['connection', 'tag', 'latest_only', 'last_read'])
        tagrows = [(f"[{tag['connection']}]{tag['tag']}", tag['latest_only'], tag['last_read']) for tag in res]
        updates = {}
        print(len(tagrows))
        for tagrow in tagrows:
            tag, latest_only, last_read = tagrow
            if latest_only:
                query =lambda session: session.query(DataTable).filter(DataTable.tagname == tag).order_by(desc(DataTable.timestamp)).limit(1).all()
                res = self.add_query(query, cols=['id', 'tagname', 'value', 'timestamp'])
                #clean out all but last row (do this in clean up)
                # if res:
                #     row = res[0]
                #     row_id = row.pop('id')
                #     updates[tag]=row
                #     self.add_query({"query": lambda session: session.query(DataTable).filter(and_(DataTable.tagname == tag, DataTable.id != row_id)).delete(),
                #                     "cols": []})
            else:
                query = lambda session: session.query(DataTable)\
                    .filter(DataTable.tagname == tag, DataTable.timestamp > last_read)\
                    .order_by(asc(DataTable.timestamp))\
                    .all()
                updates[tag]=self.add_query(query, cols=['id', 'tagname', 'value', 'timestamp'])
                if len(updates[tag]):
                    tag_only = self.parse_tagname(tag)[1] #tags are stored without [connection] in the sub table
                    #update last_read for buffered tags on sub
                    self.add_query(lambda session: session.query(SubscriptionTable)\
                                   .filter(SubscriptionTable.sub_id == sub_id)\
                                   .filter(SubscriptionTable.tag == tag_only)\
                                    .update({"last_read": ts}, synchronize_session=False))
        return updates

    def store_update(self, tag, value, timestamp=time.time()):
        """
        store_update takes a singe tag value update and puts it in the DataTable of the database

        :param tag: tagname in the [connection]tag format
        :param value: value of the tag update
        :param timestamp: timestamp of the tag value
        :return: describe what it returns 
        """ 
        self.add_query(lambda session: session.add(DataTable(tagname=tag, value=value, timestamp=timestamp)))
        

            


