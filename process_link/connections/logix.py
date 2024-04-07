import time
from pycomm3 import LogixDriver
from ..subscription import SubscriptionDb
from ..tag import Tag
from ..connection import Connection
from ..api import PropertyError

class LogixTag(Tag):

    orm = SubscriptionDb.models["tag-params-logix"]

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
        plink.add_query(lambda session: session.add(LogixTag.orm(id=params['id'],
                                            connection_id=params.get('connection_id'),                       
                                            address=params.get('address', params['id'])
                                            )))
         
class LogixConnection(Connection):

    orm = SubscriptionDb.models["connection-params-logix"]
    tag_orm = SubscriptionDb.models['tag-params-logix']
    
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
        return ['id', 'connection_id', 'description','address']
    
    @classmethod
    #adds definition to the database and is used later to instanciate
    def add_to_db(cls, plink, params):
        Connection.add_to_db(plink, params)
        plink.add_query(lambda session: session.add(cls.orm(id=params['id'],
                        pollrate=params.get('pollrate', 0.5),
                        auto_connect=params.get('auto_connect', False),
                        host=params.get('host', '127.0.0.1'),
                        port=params.get('port', 44818)
                        )))
        
    @classmethod
    def get_def_from_db(cls, plink, id):
        params = None        
        query = lambda session: session.query(cls.orm).filter(cls.orm.id == id).limit(1).all()
        res = plink.add_query(query, cols=['pollrate', 'auto_connect', 'host', 'port'])
        if res:
            params = res[0]
        return params

    def __init__(self, manager: "ProcessLink", params: dict) -> None:
        super().__init__(manager, params)
        self.properties += ['pollrate', 'auto_connect', 'host', 'port']
        self._connection_type = "logix"
        self._pollrate = params.get('pollrate', 1.0)
        self._auto_connect = params.get('auto_connect', False)
        self._port = params.get('port', 44818)
        self._host = params.get('host', '127.0.0.1')
        self.tag_properties += ['address',]
        
    def poll(self, *args):
        with LogixDriver(self.host) as plc:
            while(self.polling):
                ts = time.time()
                sub_tags = self.get_sub_tags(self.tag_properties)
                for tag in sub_tags: #used an address of the tagname if address is None
                    sub_tags[tag]['address'] = sub_tags[tag].get('address', tag)
                updates = {}
                plc_res = plc.read(*[sub_tags[t].get('address') for t in sub_tags])
                if not isinstance(plc_res, list): #result expected to be a list. if single tag, make it a list of one
                    plc_res = [plc_res,]
                for idx, tag in enumerate(sub_tags):
                    self.process_link.store_update(f"[{self._id}]{tag}", plc_res[idx].value, ts)
                time.sleep(max(0, (ts+self.pollrate)-time.time()))
