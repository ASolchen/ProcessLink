"""
https://docs.python.org/3/library/struct.html
https://docs.python.org/3/library/ctypes.html
https://en.wikipedia.org/wiki/IEC_61131-3

"""
from ctypes import c_bool, c_int8, c_uint8
from ctypes import c_int16, c_uint16, c_int32, c_uint32
from ctypes import c_int64, c_uint64, c_float, c_double

class DataType():
    """
    base class for all data types
    """
    pass

class BOOL(DataType):
    str_format = "?"
    length = 1
    modbus_registers = 1
    c_type = c_bool
    py_type = bool

class SINT(DataType):
    str_format = "b"
    length = 1
    modbus_registers = 1
    c_type = c_int8
    py_type = int

class USINT(DataType):
    str_format = "B"
    length = 1
    modbus_registers = 1
    c_type = c_uint8
    py_type = int

class INT(DataType):
    str_format = "h"
    length = 2
    modbus_registers = 1
    c_type = c_int16
    py_type = int

class UINT(DataType):
    str_format = "H"
    length = 2
    modbus_registers = 1
    c_type = c_uint16
    py_type = int

class DINT(DataType):
    str_format = "i"
    length = 4
    modbus_registers = 2
    c_type = c_int32
    py_type = int

class UDINT(DataType):
    str_format = "I"
    length = 4
    modbus_registers = 2
    c_type = c_uint32
    py_type = int

class LINT(DataType):
    str_format = "q"
    length = 8
    modbus_registers = 4
    c_type = c_int64
    py_type = int

class ULINT(DataType):
    str_format = "Q"
    length = 8
    modbus_registers = 4
    c_type = c_uint64
    py_type = int

class REAL(DataType):
    str_format = "f"
    length = 4
    modbus_registers = 2
    c_type = c_float
    py_type = float

class LREAL(DataType):
    str_format = "d"
    length = 8
    modbus_registers = 4
    c_type = c_double
    py_type = float


DATA_TYPES = {
    "BOOL": BOOL,
    "SINT": SINT,
    "USINT": USINT,
    "INT": INT,
    "UINT": UINT,
    "DINT": INT,
    "UDINT": UINT,
    "LINT": LINT,
    "ULINT": ULINT,
    "REAL": REAL,
    "LREAL": LREAL,
}