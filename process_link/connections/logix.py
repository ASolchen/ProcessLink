import time
from pycomm3 import LogixDriver
from ..subscription import SubscriptionDb
from ..tag import Tag
from ..connection import Connection
from ..api import PropertyError

class LogixTag(Tag):

    orm = SubscriptionDb.models["tag-params-logix"]

    ####################################New
    @property
    def address(self) -> str:
        return self._address
    @address.setter
    def address(self, value: str) -> None:
        self._address = value
    ####################################New

    @classmethod
    def get_params_from_db(cls, session, id: str, connection_id: str):
        params = super().get_params_from_db(session, id, connection_id)
        orm = SubscriptionDb.models["tag-params-logix"]
        tag = session.query(orm).filter(orm.id == id).filter(orm.connection_id == connection_id).first()
        if tag:
            params.update({
                'address': tag.address,
            })
        return params

    @classmethod
    def add_to_db(cls, plink, params):
        Tag.add_to_db(plink, params)
        query = {"query": lambda session: session.add(LogixTag.orm(id=params['id'],
                                            connection_id=params.get('connection_id'),                       
                                            address=params.get('address', params['id'])
                                            )), 
                "cols": []}
        plink.add_query(query)
    
    def __init__(self, params: dict) -> None:
        super().__init__(params)
        self.properties += ['address']
        self._tag_type = "logix"
        try:
            self._address = params['address']
        except KeyError as e:
            raise PropertyError(f"Missing expected property {e}")
    
    def save_to_db(self, session: "db_session") -> int:
        id = super().save_to_db(session)
        entry = session.query(self.orm).filter(self.orm.id == id).filter(self.orm.connection_id == self.connection_id).first()
        if entry == None:
            entry = self.orm()
        entry.id = self.id
        entry.address = self.address
        entry.connection_id = self.connection_id
        session.add(entry)
        session.commit()
        return entry.id
        

class LogixConnection(Connection):

    orm = SubscriptionDb.models["connection-params-logix"]
    
    @property
    def pollrate(self) -> float:
        return self._pollrate
    @pollrate.setter
    def pollrate(self, value: float) -> None:
        self._pollrate = value

    @property
    def auto_connect(self) -> bool:
        return self._auto_connect
    @auto_connect.setter
    def auto_connect(self, value: bool) -> None:
        self._auto_connect = value

    @property
    def host(self) -> str:
        return self._host
    @host.setter
    def host(self, value: str) -> None:
        self._host = value

    @property
    def port(self) -> int:
        return self._port
    @port.setter
    def port(self, value: int) -> None:
        self._port = value

    @classmethod
    def get_params_from_db(cls, session, id: str):
        params = super().get_params_from_db(session, id)
        orm = SubscriptionDb.models["connection-params-logix"]
        conn = session.query(orm).filter(orm.id == id).first()
        if conn:
            params.update({
                'pollrate': conn.pollrate,
                'auto_connect': conn.auto_connect,
                'host': conn.host,
                'port': conn.port,
            })
        return params
    
    @classmethod
    def return_tag_parameters(cls, *args):
        return ['id', 'connection_id', 'description','datatype','tag_type','address']
    
    @classmethod
    def add_to_db(cls, plink, params):
        Connection.add_to_db(plink, params)
        query = {"query": lambda session: session.add(LogixConnection.orm(id=params['id'],
                                                                    pollrate=params.get('pollrate', 0.5),
                                                                    auto_connect=params.get('auto_connect', False),
                                                                    host=params.get('host', '127.0.0.1'),
                                                                    port=params.get('port', 44818)
                                                                    )), 
                "cols": []}
        plink.add_query(query)

    def __init__(self, manager: "ProcessLink", params: dict) -> None:
        super().__init__(manager, params)
        self.properties += ['pollrate', 'auto_connect', 'host', 'port']
        self._connection_type = "logix"
        
        self._pollrate = params.get('pollrate') or 1.0
        self._auto_connect = params.get('auto_connect') or False
        self._port = params.get('port') or 44818
        self._host = params.get('host') or '127.0.0.1'
        query = {"query": lambda session: session.add(self.orm(pollrate=self.pollrate, auto_connect=self.auto_connect, host=self.host, port=self.port)),
                        "cols": []        
            }
        self.process_link.add_query(query)


    def poll(self, *args):
        with LogixDriver(self.host) as plc:
            while(self.polling):
                ts = time.time()
                while(self.thread_lock):
                    time.sleep(0.001)
                self.thread_lock = True
                sub_tags= {}
                for tag in self.polled_tags:
                    sub_tags[tag] = self.tags.get(self.process_link.parse_tagname(tag)[1]).address
                updates = {}
                plc_res = plc.read(*[addr for t, addr in sub_tags.items()])
                if not isinstance(plc_res, list): #result expected to be a list. if single tag, make it a list of one
                    plc_res = [plc_res,]
                for idx, tag in enumerate(sub_tags):
                    if not tag in updates:
                        updates[tag] = []
                    updates[tag].append((plc_res[idx].value, ts))
                self.process_link.update_handler.store_updates(updates)
                self.thread_lock = False
                time.sleep(max(0, (ts+self.pollrate)-time.time()))
