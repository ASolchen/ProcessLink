from data_manager import *


if __name__ == "__main__":
    dm = DataManager()
    c = dm.new_connection({"name": "Fred", "description": "fred's connection"})
    c.new_tag({"name": "Barney", "description": "Barney's tag"})
    print(dm.get("connections")) # one that works
    for key, con in dm.get("connections").items():
        print(con.get("tags"))