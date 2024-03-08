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
        return self._datatype
    @datatype.setter
    def datatype(self, value: str) -> None:
        self._datatype = value
    @property
    def word_swapped(self) -> bool:
        return self._word_swapped
    @word_swapped.setter
    def word_swapped(self, value: bool) -> None:
        self._word_swapped = value
    @property
    def byte_swapped(self) -> bool:
        return self._byte_swapped
    @byte_swapped.setter
    def byte_swapped(self, value: bool) -> None:
        self._byte_swapped = value
    @property
    def bit(self) -> int:
        return self._bit
    @bit.setter
    def bit(self, value: int) -> None:
        self._bit = value
    @property
    def func_type(self) -> int:
        return self._func_type
    @func_type.setter
    def func_type(self, value: int) -> None:
        self._func_type = value
    ####################################New

    @classmethod
    def get_params_from_db(cls, session, id: str, connection_id: str):
        params = super().get_params_from_db(session, id, connection_id)
        orm = ConnectionDb.models["tag-params-modbus"]
        tag = session.query(orm).filter(orm.id == id).filter(orm.connection_id == connection_id).first()
        if tag:
            params.update({
                'address': tag.address,
                'word_swapped': tag.word_swapped,
                'byte_swapped': tag.byte_swapped,
                'bit': tag.bit,
                'func_type':tag.func_type,
            })
        orm2 = ConnectionDb.models["tag-params-local"]
        tag = session.query(orm2).filter(orm2.id == id).filter(orm2.connection_id == connection_id).first()
        if tag:
            params.update({
                'datatype':tag.datatype 
            })
        return params
    
    def __init__(self, params: dict) -> None:
        super().__init__(params)
        self.properties += ['address','word_swapped','byte_swapped','bit','datatype','func_type']
        self._tag_type = "modbus"
        self.orm = ConnectionDb.models['tag-params-modbus']
        self._datatype = params.get('datatype') or 'REAL'
        self._word_swapped = params.get('word_swapped') or False
        self._byte_swapped = params.get('byte_swapped') or False
        self._bit = params.get('bit') or 1
        self._dt = params.get('datatype') or 'INT'
        self._func_type = params.get('func_type') or 4
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
        entry.word_swapped = self.word_swapped
        entry.byte_swapped = self.byte_swapped
        entry.bit = self.bit
        entry.func_type = self.func_type
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

    @property
    def station_id(self) -> int:
        return self._station_id
    @station_id.setter
    def station_id(self, value: int) -> None:
        self._station_id = value

    @property
    def status(self) -> bool:
        return self._status
    @status.setter
    def status(self, value: bool) -> None:
        self._status = value

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
                'station_id': conn.station_id,
                'status':conn.status,
            })
        return params

    def __init__(self, manager: "ProcessLink", params: dict) -> None:
        super().__init__(manager, params)
        self.properties += ['pollrate', 'auto_connect', 'host', 'port', 'station_id','status']
        self._connection_type = "modbusTCP"
        self.orm = ConnectionDb.models["connection-params-modbusTCP"]
        self._pollrate = params.get('pollrate') or 1.0
        self._auto_connect = params.get('auto_connect') or False
        self._port = params.get('port') or 502
        self._host = params.get('host') or '127.0.0.1'
        self._station_id = params.get('station_id') or 1
        self._status = params.get('status') or False

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
        entry.station_id = self.station_id
        session.add(entry)
        session.commit()
        return entry.id

    def return_tag_parameters(self,*args):
        return ['id', 'connection_id', 'description','datatype','tag_type','address','word_swapped','byte_swapped','bit','value','func_type']

    def poll(self, *args):
        try:
            with ModbusTcpClient(self.host) as plc:
                while(self.polling):
                    self._status = True
                    ts = time.time()
                    while(self.thread_lock):
                        time.sleep(0.001)
                    self.thread_lock = True
                    #sub_tags= {}
                    mod_tags = {}
                    #reg_addresses = []
                    #####
                    mb_polling = ModbusPolling()
                    #MBtag = ModbusTag()
                    #####
                    for full_tag in self.polled_tags:
                        name = (self.process_link.parse_tagname(full_tag)[1])
                        address = self.tags.get(name).address
                        ft = self.tags.get(name).func_type
                        dt = self.tags.get(name).datatype
                        print('tag',address,ft,dt,name)
                        #sub_tags[full_tag] = address
                        #reg_addresses.append(address)
                        ################
                        MBtag = ModbusTag(int(address),datatype=str(dt), func_type=int(ft), name=str(name),f_tag = full_tag )
                        mb_polling.add_tag(MBtag)
                        mod_tags[address] = MBtag
                        print(f"pollgroups {mb_polling.poll_groups}",mb_polling.tags[0].func_type)
                        #####################
                    #reg_addresses = sorted(reg_addresses)
                    #read_len = (reg_addresses[-1]+1) - reg_addresses[0] + 1
                    updates = {}
                    #################################
                    for key in mb_polling.poll_groups:
                        read_length = mb_polling.poll_groups[key][0][1]
                        start_addr = mb_polling.poll_groups[key][0][0]
                        #print ('key',key,mb_polling.poll_groups[key],read_length,start_addr)
                        ###########################Need check functionality for all 4 reads
                        if key == 3: #'HOLDING REGISTER'
                            if read_length > 125:
                                print('Modbus Holding Register Limit Exceeded')
                            try:
                                read = plc.read_holding_registers(start_addr, read_length, unit=self.station_id)
                                for tag in mb_polling.tags:     #Read Through polling tag list to parse out data from latest poll
                                    if tag.func_type == key: 
                                        if tag.addr >= start_addr and ((tag.addr + tag.num_regs) <= (start_addr + read_length)):    #Verify tag is in poll data
                                            print("tag",tag.addr,tag.num_regs)
                                            offset = tag.addr - start_addr
                                            val =  mb_polling.parse_value(read.registers[offset:offset+tag.num_regs], tag.dt)
                                            print('data',val,ts,"*",tag.ft)
                                            updates[tag.ft] = [(val,ts),]
                            except Exception as e:
                                print('Read Error ',(e))
                        elif key == 4: #'INPUT REGISTER'
                            if read_length > 125:
                                print('Modbus Holding Register Limit Exceeded')
                            try:
                                read = self.plc.read_input_registers(start_addr, read_length, unit=self.station_id)
                                for tag in mb_polling.tags:     #Read Through polling tag list to parse out data from latest poll
                                    if tag.func_type == key: 
                                        if tag.addr >= start_addr and ((tag.addr + tag.num_regs) <= (start_addr + read_length)):    #Verify tag is in poll data
                                            print("tag",tag.addr,tag.num_regs)
                                            offset = tag.addr - start_addr
                                            val =  mb_polling.parse_value(read.registers[offset:offset+tag.num_regs], tag.dt)
                                            print('data',val,ts,"*",tag.ft)
                                            updates[tag.ft] = [(val,ts),]
                            except Exception as e:
                                print('Read Error ',(e))
                        elif key == 1: #'COIL'
                            if read_length > 2000:
                                print('Modbus Coil Read Limit Exceeded')
                            try:
                                read = self.plc.read_coils(start_addr, read_length, unit=self.station_id)
                                for tag in mb_polling.tags:     #Read Through polling tag list to parse out data from latest poll
                                    if tag.func_type == key: 
                                        if tag.addr >= start_addr and ((tag.addr + tag.num_regs) <= (start_addr + read_length)):    #Verify tag is in poll data
                                            print("tag",tag.addr,tag.num_regs)
                                            offset = tag.addr - start_addr
                                            val =  (read.bits[offset])
                                            print('data',val,ts,"*",tag.ft)
                                            updates[tag.ft] = [(val,ts),]
                            except Exception as e:
                                print('Read Error ',(e))
                        elif key == 2: #'INPUT'
                            if read_length > 2000:
                                print('Modbus Input Coil Limit Exceeded')
                            try:
                                read = self.read_discrete_inputs(start_addr, read_length, unit=self.station_id)
                                for tag in mb_polling.tags:     #Read Through polling tag list to parse out data from latest poll
                                    if tag.func_type == key: 
                                        if tag.addr >= start_addr and ((tag.addr + tag.num_regs) <= (start_addr + read_length)):    #Verify tag is in poll data
                                            print("tag",tag.addr,tag.num_regs)
                                            offset = tag.addr - start_addr
                                            val =  (read.bits[offset])
                                            print('data',val,ts,"*",tag.ft)
                                            updates[tag.ft] = [(val,ts),]
                            except Exception as e:
                                print('Read Error ',(e))
                    #############################
                    # print('addresses',reg_addresses,read_len,mb_polling.poll_groups)
                    #result = plc.read_holding_registers(reg_addresses[0],read_len, unit = self.station_id)
                    #for full_tag in self.polled_tags:
                    #    tag = self.tags.get(self.process_link.parse_tagname(full_tag)[1])
                    #    addr = tag.address
                    #    offset = tag.address - reg_addresses[0]
                    #    val = struct.unpack('>f', struct.pack(">HH", *result.registers[offset:offset+2]))[0] # for float type
                    #    updates[full_tag] = [(val,ts),]
                    #    print('updates',updates[full_tag],self.polling,full_tag)
                    #
                    self.process_link.update_handler.store_updates(updates)
                    self.thread_lock = False
                    time.sleep(max(0, (ts+self.pollrate)-time.time()))
                    print('polling loop',self._status)
                self._status = False
            self._status = False
        except Exception as e:
            self.polling = False
            self._status = False
            print('connection failure', e)

    def is_polling(self,conx_id,*args):
        #print('is_polling method',self.process_link.is_polling(conx_id),self._status,self.polling)
        return(self.process_link.is_polling(conx_id))

'''
    def update_group(self, group):
        #reads appropriate modbus table and return raw bytes
        function = group[0].function  # should be same for all tags
        read_range = [group[0].addr, group[0].addr+1]
        for tag in group:
            if read_range[0] > tag.addr:
                read_range[0] = tag.addr
            if read_range[1] <= tag.addr + 1:
                tag_regs = 1
                if tag.data_type in ['FLOAT', 'UNINT32', 'INT32']:
                    tag_regs = 2
                read_range[1] = tag.addr + tag_regs
        read_len = read_range[-1] - read_range[0]
        if function == 'HOLDING REGISTER':
            if read_len > 125:
                raise Exception('Modbus Holding Register Limit Exceeded')
            read = self.plc.read_holding_registers(read_range[0], read_len, unit=self.unit)
            if isinstance(read, ExceptionResponse):
                raise(Exception('{} Read Error'.format(function)))
            data =  struct.pack('H' * read_len, *read.registers)
        elif function == 'INPUT REGISTER':
            if read_len > 125:
                raise Exception('Modbus Input Register Limit Exceeded')
            read = self.plc.read_input_registers(read_range[0], read_len, unit=self.unit)
            if isinstance(read, ExceptionResponse):
                raise(Exception('{} Read Error'.format(function)))
            data = struct.pack('H'*read_len, *read.registers)
        elif function == 'COIL':
            if read_len > 2000:
                raise Exception('Modbus Coil Limit Exceeded')
            read = self.plc.read_coils(read_range[0], read_len, unit=self.unit)
            if isinstance(read, ExceptionResponse):
                raise(Exception('{} Read Error'.format(function)))
            data = struct.pack('B' * len(read.bits), *read.bits)  # TODO no idea if this is correct
        elif function == 'INPUT STATUS':
            if read_len > 2000:
                raise Exception('Modbus Input Status Limit Exceeded')
            read = self.read_discrete_inputs(read_range[0], read_len, unit=self.unit)
            if isinstance(read, ExceptionResponse):
                raise(Exception('{} Read Error'.format(function)))
            data = struct.pack('B' * len(read.bits), *read.bits)  # TODO no idea if this is correct
        ts = time.time()  # used for to group time stamp
        # unpack the data
        for tag in group:
            dt = tag.data_type
            if function in ['HOLDING REGISTER', 'INPUT REGISTER']:
                offset = (tag.addr - read_range[0]) * 2 # get the byte offset for the tag in the data
                data_bytes = data[offset:offset + 2]
                if dt in ['UINT16', 'INT16']:
                    if tag.byte_swapped:
                        data_bytes = data_bytes[1] + data_bytes[0]
                if dt in ['UINT32', 'INT32', 'FLOAT']:
                    data_bytes = data[offset:offset + 4]
                    if tag.byte_swapped:
                        data_bytes = data_bytes[1] + data_bytes[0] + data_bytes[3] + data_bytes[2]
                    if tag.word_swapped:
                        data_bytes = data_bytes[2:4] + data_bytes[0:2]
                if dt == 'BOOL':
                    uint_reg_val = struct.unpack('H', data_bytes)[0]
                    bit = tag.bit
                    value = (uint_reg_val & (1 << bit)) >> bit
                elif dt == 'INT16':
                    value = struct.unpack('h', data_bytes)[0]
                elif dt == 'UINT16':
                    value = struct.unpack('H', data_bytes)[0]
                elif dt == 'INT32':
                    value = struct.unpack('i', data_bytes)[0]
                elif dt == 'UINT32':
                    value = struct.unpack('I', data_bytes)[0]
                elif dt == 'FLOAT':
                    value = struct.unpack('f', data_bytes)[0]
            else:
                offset = tag.addr - read_range[0]
                value = struct.unpack('B', data[offset])[0]
            tag.add_data((ts, value))
'''


import struct

class datatype():
    length = 1
    format_code = 'x'
    @classmethod
    def to_bytes(cls, val):
        return struct.pack(cls.format_code, val)
    
    @classmethod
    def from_bytes(cls, data):
        return struct.unpack(cls.format_code*(len(data) / cls.length))
    
    @classmethod
    def bit_read(cls, val, bit):
        return bool(val>>bit &0x01)

class dt_lreal(datatype):
    length = 8
    format_code = 'd'

class dt_ulint(datatype):
    length = 8
    format_code = 'Q'

class dt_lint(datatype):
    length = 8
    format_code = 'q'

class dt_real(datatype):
    length = 4
    format_code = 'f'

class dt_udint(datatype):
    length = 4
    format_code = 'I'

class dt_dint(datatype):
    length = 4
    format_code = 'i'

class dt_uint(datatype):
    length = 2
    format_code = 'H'

class dt_int(datatype):
    length = 2
    format_code = 'h'

class dt_usint(datatype):
    length = 1
    format_code = 'B'

class dt_sint(datatype):
    length = 2
    format_code = 'b'

datatypes = {
    'LREAL': dt_lreal,
    'ULINT': dt_ulint,
    'LINT': dt_lint,
    'REAL': dt_real,
    'UDINT': dt_udint,
    'DINT': dt_dint,
    'UINT': dt_uint,
    'INT': dt_int,
    'USINT': dt_usint,
    'SINT': dt_sint,
    'BOOL': None
}


class ModbusTag():
    
    def __init__(self, addr, datatype='UINT', func_type=3, name="", f_tag = ""):
        self.addr = addr
        self.dt = datatype
        self.ft = f_tag
        num_regs = 1 if self.dt == 'BOOL' else int(datatypes[self.dt].length / 2) # 2 bytes per reg
        self.num_regs = 1 if self.dt == 'BOOL' else int(datatypes[self.dt].length / 2) # 2 bytes per reg
        self.regs = list(range(addr, addr+num_regs))
        self.func_type = func_type #0= Coils, 1=Discrete Inputs, 3=Input Registers, 4=Holding Registers
    

class ModbusPolling():

    def __init__(self, tags=None) -> None:
        self.poll_groups = {}
        self.tags = []
        if tags:
            for tag in tags:
                self.add_tag(tag)
        

    def optimize(self):
        self.poll_groups = self.optimize_polls(self.tags)

    def add_tag(self, mb_tag):
        self.tags.append(mb_tag)
        self.optimize()

    @staticmethod
    def optimize_poll(mb_tags):
        #send in the tags, return tuples of (address, length) for poll groups of a single function type
        int_list = []
        for tag in mb_tags:
            int_list.extend(tag.regs)
        int_list.sort()  # sort the list of integers
        groups = []
        current_group = [int_list[0]]
        for i in range(1, len(int_list)):
            if int_list[i] - current_group[0] <= 125:
                current_group.append(int_list[i])
            else:
                groups.append(tuple(current_group))
                current_group = [int_list[i]]
        groups.append(tuple(current_group))
        poll_groups = []
        for g in sorted(groups):
            poll_groups.append((g[0], g[-1]-g[0]+1))
        return poll_groups
    
    @staticmethod
    def optimize_polls(tags):
        tag_groups = {}
        for tag in tags:
            if not tag_groups.get(tag.func_type):
                tag_groups[tag.func_type] = []
            tag_groups[tag.func_type].append(tag)
        poll_groups = {}
        for g in sorted(tag_groups):
            poll_groups[g] = ModbusPolling.optimize_poll(tag_groups[g])
        return poll_groups
    
    @staticmethod
    def parse_value(regs: list[int], format: str):
        val = None
        try:
            data = struct.pack(">"+ datatypes['UINT'].format_code * len(regs), *regs)
            val = struct.unpack(">"+ datatypes[format].format_code, data)[0]
        except KeyError as e:
            print(f'Unknow datatype format: {format}- {e}')
        return val

tags =  {'PC_Manual_Mode': {'address': 0, },
        'DHW_Mode': False,
        'CH_Mode': False,
        'Freeze_Protection_Mode': False,
        'Flame_Present': False,
        'CH_1_Pump': False,
        'DHW_Pump': False,
        'Sys_CH_2_Pump': False,
        'Auto_Lockout': False,
        'Manual_Lockout': False,
        'Boiler_Supply_Temp': 0.0,
        'Boiler_Return_Temp': 0.0,
        'DHW_Storage_Temp': 0.0,
        'Boiler_Flue_Temp': 0.0,
        'Outdoor_Temp': 0.0
}

t = [ModbusTag(768,'INT',func_type=4, name='Boiler_Supply_Temperature'),
     ModbusTag(769,'INT',func_type=4, name='Boiler_Return_Temperature'),
     ModbusTag(770,'INT',func_type=4, name='DHW_Storage_Temperature'),
     ModbusTag(771,'INT',func_type=4, name='Boiler_Flue_Temperature'),
     ModbusTag(772,'INT',func_type=4, name='Outdoor_Temperature'),
     ModbusTag(774,'INT',func_type=4, name='Flame_Ionization_Current'),
     ModbusTag(775,'INT',func_type=4, name='Boiler_Fire_Rate'),
     ]

#create polling by list
mb_polling = ModbusPolling(tags=t)
print(f"pollgroups {mb_polling.poll_groups}")

#add a couple coil addresses in random areas
mb_polling.add_tag(ModbusTag(2,'BOOL',func_type=1, name='Boiler_Running'),)
mb_polling.add_tag(ModbusTag(2466,'BOOL',func_type=1, name='Boiler_Alarm'),)
print(f"pollgroups {mb_polling.poll_groups}")

#add a couple input register addresses as 'REAL' types (2 regs each)
mb_polling.add_tag(ModbusTag(2,'REAL',func_type=3, name='Water_Temp'),)
mb_polling.add_tag(ModbusTag(4,'REAL',func_type=3, name='Flame_Temp'),)
print(f"pollgroups {mb_polling.poll_groups}")

print(f"Test bit read. Read bit 3 of 0b0000_0000_0000_1010 (dec 10): {datatype.bit_read(10, 0)}")
print(f"Test bit read. Read bit 3 of 0b0000_0000_0000_1010 (dec 10): {datatype.bit_read(10, 1)}")
print(f"Test bit read. Read bit 3 of 0b0000_0000_0000_1010 (dec 10): {datatype.bit_read(10, 2)}")
print(f"Test bit read. Read bit 3 of 0b0000_0000_0000_1010 (dec 10): {datatype.bit_read(10, 3)}")


#modbus regs come back as unsigned int (UINT) values
regs = [4048, 16457]
print(f"Packed regs 4048, 16457 as 'REAL' : {ModbusPolling.parse_value(regs, 'REAL')}")