from data_manager import DataManager

if __name__ == "__main__":
    dm = DataManager()
    dm.set("db_file", "test.db")
    dm.load_db()
    for x in range(10):
        c = dm.new_connection({"id": f"Fred{x}", "connection_type": "base", "description": "fred's connection"})

    print(dm.get("connections")) # one that works
    #print(dm.get("bad_prop")) # one that fails
    for conn_id, conn_obj in dm.get("connections").items():
        dm.save_connection(conn_obj)
        print(conn_obj)
        conn_id = conn_obj.get("id")
        for x in range(10):
            conn_obj.new_tag({"id": f"Tag{x}", "connection_id": conn_id, "description": f"Blah Blah {1+x}", "value":x*100.0 })
        for tag_id, tag_obj in conn_obj.get("tags").items():
            dm.save_tag(tag_obj)
            print(tag_obj)
        for tag_id, tag_obj in conn_obj.get("tags").items():
            dm.save_tag(tag_obj)
            print(tag_obj)
    
    dm.close_db()