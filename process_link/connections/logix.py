import time
from pycomm3 import LogixDriver
from ..database import ConnectionDb
from ..tag import Tag
from ..connection import Connection
from ..api import PropertyError

class LogixTag(Tag):
    @property
    def address(self) -> str:
        return self._address

    @classmethod
    def get_params_from_db(cls, session, id: str, connection_id: str):
        params = super().get_params_from_db(session, id, connection_id)
        orm = ConnectionDb.models["tag-params-logix"]
        tag = session.query(orm).filter(orm.id == id).filter(orm.connection_id == connection_id).first()
        if tag:
            params.update({
                'address': tag.address,
            })
        return params
    
    def __init__(self, params: dict) -> None:
        super().__init__(params)
        self.properties += ['address']
        self._tag_type = "logix"
        self.orm = ConnectionDb.models['tag-params-logix']
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
    def port(self, value: str) -> None:
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
        orm = ConnectionDb.models["connection-params-logix"]
        conn = session.query(orm).filter(orm.id == id).first()
        if conn:
            params.update({
                'pollrate': conn.pollrate,
                'auto_connect': conn.auto_connect,
                'host': conn.host,
                'port': conn.port,
            })
        return params

    def __init__(self, manager: "ProcessLink", params: dict) -> None:
        super().__init__(manager, params)
        self.properties += ['pollrate', 'auto_connect', 'host', 'port']
        self._connection_type = "logix"
        self.orm = ConnectionDb.models["connection-params-logix"]
        self._pollrate = params.get('pollrate') or 1.0
        self._auto_connect = params.get('auto_connect') or False
        self._port = params.get('port') or 44818
        try:
            self._host = params['host']
        except KeyError as e:
            raise PropertyError(f"Missing expected property {e}")

    def save_to_db(self, session: "db_session") -> str:
        id = super().save_to_db(session)
        entry = session.query(self.orm).filter(self.orm.id == id).first()
        if entry == None:
            entry = self.orm()
        entry.id = self.id
        entry.pollrate = self.pollrate
        entry.auto_connect = self.auto_connect
        entry.host = self.host
        entry.port = self.port
        session.add(entry)
        session.commit()
        return entry.id

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
                for idx, tag in enumerate(sub_tags):
                    if not tag in updates:
                        updates[tag] = []
                    updates[tag].append((plc_res[idx].value, ts))
                self.process_link.update_handler.store_updates(updates)
                self.thread_lock = False
                time.sleep(max(0, (ts+self.pollrate)-time.time()))