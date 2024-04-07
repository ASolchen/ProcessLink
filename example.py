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
                                "host": '192.168.30.169',
                                "pollrate": 0.2,
                                "station_id": 1
                                })
        
    link.new_tag({"id":"Crap",
                  "tag_type": "modbusTCP",
                  "connection_id":"MB_Test",
                  "func": 1,
                  "bit": 0,
                  "address": "1"})
    
    link.subscribe("Display01", "[HousePLC]TankLevel", latest_only=False)
    link.subscribe("Display01", "[Random]Crap", latest_only=False)
    #link.subscribe("Display01", "[MB_Test]Crap", latest_only=False)
    for x in range(10):
        print(link.get_tag_updates("Display01"))
        time.sleep(0.5)

    link.subscribe("Display01", "[HousePLC]NewTag", latest_only=False)

    for x in range(10):
        print(link.get_tag_updates("Display01"))
        time.sleep(0.5)
    # result = link.query_attempt()
    # print(result)