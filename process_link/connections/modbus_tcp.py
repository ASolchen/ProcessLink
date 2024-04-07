import time, struct
from pymodbus.client import ModbusTcpClient
from ..subscription import SubscriptionDb
from ..tag import Tag
from ..connection import Connection
from ..api import PropertyError

class FuncTable(): #from mb function read codes, used in tag db
    COILS = 1
    DISCRETES = 2
    HOLDING_REGS = 3
    STAT_REGS = 4


class ModbusTag(Tag):
    orm = SubscriptionDb.models['tag-params-modbus']
    @classmethod
    def get_params_from_db(cls, session, id: str, connection_id: str):
        params = super().get_params_from_db(session, id, connection_id)
        orm = SubscriptionDb.models["tag-params-modbus"]
        tag = session.query(orm).filter(orm.id == id).filter(orm.connection_id == connection_id).first()
        if tag:
            params.update({
                'address': tag.address,
            })
        return params
    

class ModbusTCPConnection(Connection):
    orm = SubscriptionDb.models['connection-params-modbusTCP']
    tag_orm = ModbusTag

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
        orm = cls.orm
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
        return ['id', 'connection_id', 'description','func','bit','address',
                'word_swapped', 'byte_swapped']

    @classmethod
    #adds definition to the database and is used later to instanciate
    def add_to_db(cls, plink, params):
        Connection.add_to_db(plink, params)
        plink.add_query(lambda session: session.add(cls.orm(id=params['id'],
                        pollrate=params.get('pollrate', 0.5),
                        auto_connect=params.get('auto_connect', False),
                        host=params.get('host', '127.0.0.1'),
                        port=params.get('port', 502),
                        station_id=params.get('station_id', 1),
                        )))
        
    @classmethod
    def get_def_from_db(cls, plink, id):
        params = None        
        query = lambda session: session.query(cls.orm).filter(cls.orm.id == id).limit(1).all()
        res = plink.add_query(query, cols=['pollrate', 'auto_connect', 'host', 'port', 'station_id'])
        if res:
            params = res[0]
        return params

    def __init__(self, manager: "ProcessLink", params: dict) -> None:
        super().__init__(manager, params)
        self.properties += ['pollrate', 'auto_connect', 'host', 'port', 'station_id']
        self._connection_type = "modbusTCP"
        self._pollrate = params.get('pollrate') or 1.0
        self._auto_connect = params.get('auto_connect') or False
        self._port = params.get('port') or 502
        self._host = params.get('host') or '127.0.0.1'
        self._host = params.get('station_id') or '1'

    def poll(self, *args):
        with ModbusTcpClient(self.host) as plc:
            while(self.polling):
                ts = time.time()
                updates = self.read_tags()
                for idx, tag in enumerate(updates):
                    pass#self.process_link.store_update(f"[{self._id}]{tag}", plc_res[idx].value, ts)
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

    def read_tags(self):
        if self.get_tags_changed():
            self.optimize_poll_groups()
        updates = self.read_coils() + \
            self.read_discretes() + \
            self.read_holding_regs() + \
            self.read_input_regs()
        return updates
    
    def optimize_poll_groups(self):
        pass #TODO
    
    def read_coils(self):
        updates = []
        #get all of the tags that are in "coils", are in this connection, and are subcribed to
        # need to join below with modbus_tag table where func = 1 for coils
        # sub_tags = {} 
        # res = self.process_link.add_query(lambda session: \
        #    session.session.query(SubscriptionTable.connection, SubscriptionTable.tag)\
        #   .join(TagParamsModbus, SubscriptionTable.tag == TagParamsModbus.id)\
        #   .filter(SubscriptionTable.connection == self._id)\
        #   .filter(TagParamsModbus.func == 1)\
        #   .all(), cols=[self.return_tag_parameters()])
        return updates