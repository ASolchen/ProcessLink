import time, struct
from pymodbus.client import ModbusTcpClient
from ..subscription import SubscriptionDb, SubscriptionTable, TagTable, TagParamsModbus
from ..tag import Tag
from ..connection import Connection
from ..api import PropertyError
from sqlalchemy import and_, asc, desc

class FuncTable(): #from mb function read codes, used in tag db
    COILS = 1
    DISCRETES = 2
    HOLDING_REGS = 3
    STAT_REGS = 4


class PollGroup():
    def __init__(self, addr, len) -> None:
        self.address = addr
        self.len = len
        self.timestamp = 0.0
        self.tags = []


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
    
    @classmethod
    def add_to_db(cls, plink, params):
        Tag.add_to_db(plink, params)
        plink.add_query(lambda session: session.add(\
            ModbusTag.orm(id=params['id'],
                connection_id=params.get('connection_id'),                       
                func=params.get('func'),                       
                address=params.get('address'),
                bit=params.get('bit', 0),
                word_swapped=params.get('word_swapped', False),
                byte_swapped=params.get('byte_swapped', False),
                )))
    
    def __init__(self, params: dict) -> None:
        super().__init__(params)
        self.tag = params.get("id")
        self.func = params.get("func")
        self.address = params.get("address")
        self.bit = params.get("bit")
        self.word_swapped = params.get("word_swapped")
        self.byte_swapped = params.get("byte_swapped")
        self.regs_required = ModbusTCPConnection.data_types[self.datatype].modbus_registers
    

class ModbusTCPConnection(Connection):
    orm = SubscriptionDb.models['connection-params-modbusTCP']
    tag_orm = ModbusTag.orm

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
        self._station_id = int(params.get('station_id',1))
        self.poll_groups = {
            0: [], #coils
            1: [], #discrete inputs
            3: [], #holding regs
            4: [], #input regs
        }
        self.mb_client = None

    def poll(self, *args):
        with ModbusTcpClient(self._host) as self.mb_client:
            while(self.polling):
                ts = time.time()
                self.read_tags()
                time.sleep(max(0, (ts+self.pollrate)-time.time()))

    def read_tags(self):
        if self.get_tags_changed():
            self.update_tags_changed(False) # clear the flag
            self.optimize_poll_groups()
        self.read_coils() 
        self.read_discretes()
        self.read_holding_regs()
        self.read_input_regs()
    
    def optimize_poll_groups(self):
        # clear the existing Pollgroups
        self.poll_groups = {
            0: [], #coils
            1: [], #discrete inputs
            3: [], #holding regs
            4: [], #input regs
        }
        join_condition = SubscriptionTable.tag == self.tag_orm.id
        res = self.process_link.add_query(lambda session: \
           session.query(SubscriptionTable.connection, SubscriptionTable.tag)\
                .join(self.tag_orm, join_condition)\
                .filter(SubscriptionTable.connection == self._id)\
                .distinct(SubscriptionTable.tag)\
                .order_by(asc(self.tag_orm.address))\
                .all(), cols=["tag"])
        if len(res):
            tags = [r.get("tag") for r in res]
            for tag in tags: # add each to a Pollgroup
                join_condition = and_(TagTable.id == TagParamsModbus.id, TagTable.id == tag)
                res = self.process_link.add_query(lambda session: \
                    session.query(TagTable.id.label("id"),
                                  TagTable.datatype.label("datatype"),
                                   TagParamsModbus.func.label("func"),
                                   TagParamsModbus.address.label("address"),
                                   TagParamsModbus.bit.label("bit"),
                                   TagParamsModbus.word_swapped.label("word_swapped"),
                                   TagParamsModbus.byte_swapped.label("byte_swapped"))\
                        .join(TagTable, join_condition)\
                        .order_by(asc(TagParamsModbus.address))\
                        .all(), cols=["id", "datatype", "func", "address",
                                      "bit", "word_swapped", "byte_swapped"])
                if len(res):
                    params=res[0]
                    params["connection_id"] = self._id
                    tag_obj = ModbusTag(params)
                    if tag_obj.func in [3,4]:
                        need_new_pg = not len(self.poll_groups[tag_obj.func]) or\
                            self.poll_groups[tag_obj.func][-1].address + 125 <  tag_obj.address + tag_obj.regs_required
                        if need_new_pg:
                            self.poll_groups[tag_obj.func].append(PollGroup(tag_obj.address, tag_obj.regs_required))
                        pg = self.poll_groups[tag_obj.func][-1]
                        pg.len = (tag_obj.address + tag_obj.regs_required) - pg.address
                        pg.tags.append(tag_obj)
        
    
    def read_coils(self):
        updates = []
        return updates
    
    def read_discretes(self):
        updates = []
        return updates
    

    def read_holding_regs(self):
        self.read_reg("holding")

    def read_input_regs(self):
        self.read_reg("input")

    def read_reg(self, reg_type):
        func = 3 if reg_type=="input" else 4
        meth = self.mb_client.read_input_registers if reg_type=="input" else self.mb_client.read_holding_registers
        #update the data in the polling groups
        for idx, pg in enumerate(self.poll_groups[func]):
            mb_read = meth(pg.address , pg.len, self._station_id)
            pg.timestamp = time.time()
            try:
                data = struct.pack(self.data_types["UINT"].str_format * len(mb_read.registers), *mb_read.registers)
            except AttributeError:
                print(mb_read)
            for tag_idx, tag in enumerate(pg.tags):
                try:
                    fmt = self.data_types[tag.datatype].str_format
                    start = (tag.address - pg.address) * 2
                    data_len = self.data_types[tag.datatype].length
                    #TODO should do something with word_swapped, byte_swapped and deal with BOOL type / bit here
                    value = struct.unpack(fmt, data[start:start+data_len])[0]
                    self.process_link.store_update(tag.tagname, value, timestamp=pg.timestamp)
                except Exception as e:
                    raise Exception(f"Error parsing {reg_type} register data in Modbus connection: {self.id}")

    

    