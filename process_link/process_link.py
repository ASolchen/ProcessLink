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
from sqlalchemy import desc, and_
####################Different
from pycomm3 import tag
#####################
from .api import APIClass, PropertyError

from .connection import Connection, TAG_TYPES, UnknownConnectionError
from .tag import Tag
from .database import ConnectionDb, DatabaseError
from .subscription import SubscriptionTable, DataTable

from .subscription import SubscriptionDb, UpdateHandler
__all__ = ["ProcessLink"]

CONNECTION_TYPES = {'local': Connection}
from .connections.logix import LogixConnection, LogixTag
CONNECTION_TYPES['logix'] =  LogixConnection
TAG_TYPES['logix'] =  LogixTag
from .connections.modbus_tcp import ModbusTcpTag, ModbusTCPConnection
CONNECTION_TYPES['modbusTCP'] =  ModbusTCPConnection
TAG_TYPES['modbusTCP'] =  ModbusTcpTag


class ProcessLink(APIClass):

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

    def _query_thread(self):
        self.db = SubscriptionDb()
        while True:
            query_info, result_queue, query_ready_event = self.query_queue.get()
            query = query_info['query']
            args = query_info['args']
            kwargs = query_info['kwargs']

            # Execute the query within the session
            result = self.db.run_query(query)

            # Put the result into the result queue
            result_queue.put(result)

            # Set the event to indicate that the query has been executed
            query_ready_event.set()

    def add_query(self, query, *args, **kwargs):
        thread_id = threading.current_thread().ident  # Generate thread ID
        result_queue = queue.Queue()
        query_ready_event = threading.Event()

        # Add the result queue and event to the dictionary
        self.result_queues[thread_id] = (result_queue, query_ready_event)

        # Add the query information and related queue/event to the main queue
        query_info = {
            'query': query,
            'args': args,
            'kwargs': kwargs
        }
        self.query_queue.put((query_info, result_queue, query_ready_event))

        # Wait for the query to be executed by the other thread
        query_ready_event.wait()

        # Get the result from the result queue associated with this thread
        result = result_queue.get()

        # Clear the event for the next query
        query_ready_event.clear()

        return result

    def query_attempt(self):
        # sub_id = Column(String, nullable=False)
        # tagname = Column(String, nullable=False)
        # last_read = Column(Float, default=0.0)
        # latest_only = Column(Boolean) #if True, latest value overwrites else all are buffered between updates
        # for x in range(100):
        #     query = {"query": lambda session: session.add(SubscriptionTable(sub_id="Sub", tagname=f"fred{x}", last_read=0, latest_only=False)),
        #                 "cols": ["tagname", "sub_id"]
        #     }
        #     self.add_query(query)
        t = time.time()
        self.store_update("[HousePLC]TankLevel", t % 100, t)
        self.store_update("[HousePLC]TankLevel", t % 50, t)
        self.store_update("[HousePLC]TankLevel", t % 67, t)
        self.store_update("[HousePLC]TankLevel", t % 10 * 0.5, t)
        self.store_update("[Random]Crap", t % 10, t)
        # query = {"query": lambda session: session.add(DataTable(tagname="[Random]Crap", value=str(t), timestamp=t)),
        #          "cols": []
        #          }
        # self.add_query(query)

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
            CONNECTION_TYPES[params['connection_type']].add_to_db(self, params)
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
        query = {"query": lambda session: session.add(SubscriptionTable(sub_id=sub_id, tagname=tagname, latest_only=latest_only)),
            "cols": []
            }
        self.add_query(query)

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
        else:
            raise DatabaseError("Database has not been loaded")

########################New
    def get_connection_params(self, conn: "Connection",conx_id) -> None:
        if self.db_interface.session:
            return conn.get_params_from_db(self.db_interface.session,conx_id)

    def delete_connection(self, conn: "Connection",conx_id) -> None:
        try:
            del self.connections[conx_id]
            if self.db_interface.session:
                conn.delete_from_db(self.db_interface.session,conx_id)
        except KeyError as e:
            raise PropertyError(f'Connection does not exist: {e}')

    def delete_tag(self, tag: "Tag",tag_id,conx_id) -> None:
        try:
            del self.connections[conx_id].tags[tag_id]
            if self.db_interface.session:
                tag.delete_from_db(self.db_interface.session,tag_id)
        except KeyError as e:
            raise PropertyError(f'Tag does not exist: {e}')

########################New  

    def save_tag(self, tag: "Tag") -> None:
        if self.db_interface.session:
            tag.save_to_db(self.db_interface.session)
        
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
        ts = time.time()
        #get all unique tags in the sub
        query = {"query": lambda session: session.query(SubscriptionTable).filter(SubscriptionTable.sub_id == sub_id).distinct().all()
,
                    "cols": ['tagname', 'latest_only', 'last_read']
                    }
        res = self.add_query(query)
        latest_only = True
        tagrows = [(tag['tagname'], tag['latest_only'], tag['last_read']) for tag in res]
        updates = {}
        print(len(tagrows))
        for tagrow in tagrows:
            tag, latest_only, last_read = tagrow
            if latest_only:
                query = {"query": lambda session: session.query(DataTable).filter(DataTable.tagname == tag).order_by(desc(DataTable.timestamp)).limit(1).all(),
                        "cols": ['id', 'tagname', 'value', 'timestamp']
                        }
                res = self.add_query(query)
                #clean out all but last row (do this in clean up)
                # if res:
                #     row = res[0]
                #     row_id = row.pop('id')
                #     updates[tag]=row
                #     self.add_query({"query": lambda session: session.query(DataTable).filter(and_(DataTable.tagname == tag, DataTable.id != row_id)).delete(),
                #                     "cols": []})
            else:
                query = {"query": lambda session: session.query(DataTable).filter(DataTable.tagname == tag, DataTable.timestamp > last_read).all(),
                        "cols": ['id', 'tagname', 'value', 'timestamp']
                        }
                updates[tag]=self.add_query(query)
                #update last_read for buffered tags on sub
                self.add_query({"query": lambda session: session.query(SubscriptionTable).filter(SubscriptionTable.sub_id == sub_id).update({"last_read": ts}, synchronize_session=False),
                                "cols": []})
        return updates

    def store_update(self, tag, value, timestamp=time.time()):
        """
        store_update takes a singe tag value update and puts it in the DataTable of the database

        :param tag: tagname in the [connection]tag format
        :param value: value of the tag update
        :param timestamp: timestamp of the tag value
        :return: describe what it returns 
        """ 
        query = {"query": lambda session: session.add(DataTable(tagname=tag, value=value, timestamp=timestamp)),
                "cols": []
                }
        self.add_query(query)
        

            


