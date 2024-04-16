from process_link import ProcessLink
import time


if __name__ == "__main__":
    link = ProcessLink()
    link.new_connection({"id": f"HousePLC",
                                "connection_type": "logix",
                                "description": "House PLC",
                                "host": '192.168.30.169',
                                "pollrate": 0.2
                                })
    link.new_tag({"id":"TankLevel",
                  "tag_type": "logix",
                  "connection_id":"HousePLC",
                  "address": "TankLevel"})


    link.new_connection({"id": f"MB_Test",
                                "connection_type": "modbusTCP",
                                "description": "House PLC",
                                "host": '127.0.0.1',
                                "pollrate": 0.2,
                                "station_id": 1
                                })
        
    link.new_tag({"id":"Foo",
                  "tag_type": "modbusTCP",
                  "connection_id":"MB_Test",
                  "func": 4,
                  "bit": 0,
                  "datatype": "INT",
                  "address": "1"})
        
    [link.new_tag({"id":f"MB_Tag{x}",
                  "tag_type": "modbusTCP",
                  "connection_id":"MB_Test",
                  "func": 3,
                  "bit": 0,
                  "datatype": "DINT",
                  "address": x}) for x in range(250)]
    
    link.subscribe("Display01", "[HousePLC]TankLevel", latest_only=False)
    #link.subscribe("Display01", "[Random]Crap", latest_only=False)
    link.subscribe("Display01", "[MB_Test]Foo")
    link.subscribe("Display01", "[MB_Test]Bar")
    [link.subscribe("Display02", f"[MB_Test]MB_Tag{x}") for x in range(5)]
    for x in range(10):
        print(link.get_tag_updates("Display02"))
        time.sleep(0.5)

    link.subscribe("Display01", "[HousePLC]NewTag", latest_only=False)

    for x in range(10):
        print(link.get_tag_updates("Display02"))
        time.sleep(0.5)
    # result = link.query_attempt()
    # print(result)