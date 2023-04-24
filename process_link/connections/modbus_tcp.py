import time, struct
from pymodbus.client import ModbusTcpClient
from ..database import ConnectionDb
from ..tag import Tag

from ..connection import Connection
from ..api import PropertyError

class ModbusTcpTag(Tag):
    ####################################New
    @property
    def address(self) -> int:
        return self._address
    @address.setter
    def address(self, value: int) -> None:
        self._address = value
    @property
    def datatype(self) -> str:
        return self._address
    @datatype.setter
    def datatype(self, value: str) -> None:
        self._datatype = value
    ####################################New

    @classmethod
    def get_params_from_db(cls, session, id: str, connection_id: str):
        params = super().get_params_from_db(session, id, connection_id)
        orm = ConnectionDb.models["tag-params-modbus"]
        tag = session.query(orm).filter(orm.id == id).filter(orm.connection_id == connection_id).first()
        if tag:
            params.update({
                'address': tag.address,
            })
        return params
    
    def __init__(self, params: dict) -> None:
        super().__init__(params)
        self.properties += ['address']
        self._tag_type = "modbus"
        self.orm = ConnectionDb.models['tag-params-modbus']
        self._datatype = params.get('datatype') or 'REAL'
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
        

class ModbusTCPConnection(Connection):
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
        orm = ConnectionDb.models["connection-params-modbusTCP"]
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
        self._connection_type = "modbusTCP"
        self.orm = ConnectionDb.models["connection-params-modbusTCP"]
        self._pollrate = params.get('pollrate') or 1.0
        self._auto_connect = params.get('auto_connect') or False
        self._port = params.get('port') or 502
        self._host = params.get('host') or '127.0.0.1'

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

    def return_tag_parameters(self,*args):
        return ['id', 'connection_id', 'description','datatype','tag_type','address']

    def poll(self, *args):
        with ModbusTcpClient(self.host) as plc:
            while(self.polling):
                ts = time.time()
                while(self.thread_lock):
                    time.sleep(0.001)
                self.thread_lock = True
                sub_tags= {}
                reg_addresses = []
                for full_tag in self.polled_tags:
                    address = self.tags.get(self.process_link.parse_tagname(full_tag)[1]).address
                    sub_tags[full_tag] = address
                    reg_addresses.append(address)
                reg_addresses = sorted(reg_addresses)
                read_len = (reg_addresses[-1]+1) - reg_addresses[0] + 1
                updates = {}
                result = plc.read_holding_registers(reg_addresses[0],read_len)
                
                for full_tag in self.polled_tags:
                    tag = self.tags.get(self.process_link.parse_tagname(full_tag)[1])
                    addr = tag.address
                    offset = tag.address - reg_addresses[0]
                    val = struct.unpack('f', struct.pack("HH", *result.registers[offset:offset+2]))[0] # for float type
                    updates[full_tag] = [(val,ts),]
                self.process_link.update_handler.store_updates(updates)
                self.thread_lock = False
                time.sleep(max(0, (ts+self.pollrate)-time.time()))


# self.tag_updates = {
        #   "[Conx1]Tag01": [
        #          (3.14159, 1643503017.642834), # tuples of value, timestamp
        #          (3.14159, 1643503018.642834),
        #          (3.14159, 1643503019.642834),
        #          (3.14159, 1643503020.642834), 
        #           ],
        #   "[Conx1]Tag02": [
        #          (3.14159, 1643503017.642834),
        #          (3.14159, 1643503018.642834),
        #          (3.14159, 1643503019.642834),
        #          (3.14159, 1643503020.642834), 
        #           ],
        # }