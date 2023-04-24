from pymodbus.client import ModbusTcpClient

with ModbusTcpClient('192.168.20.25') as client:
    try:
        result = client.read_holding_registers(990,100)
        print(result.registers)
    except Exception as e:
        print(e)

