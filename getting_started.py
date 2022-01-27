from data_manager import DataManager

if __name__ == "__main__":
    dm = DataManager()
    #print(dm.get('connection_types'))
    dm.set("db_file", "test.db")
    dm.load_db()
    # for x in range(10):
    #     c = dm.new_connection({"id": f"Fred{x}",
    #                             "connection_type": "logix",
    #                             "description": "fred's connection",
    #                             "host": '192.168.1.169'
    #                             })
    
    #print(dm.get("connections")) # one that works
    # #print(dm.get("bad_prop")) # one that fails
    for conn_id, conn_obj in dm.get("connections").items():
    #     dm.save_connection(conn_obj)
    #     print(conn_obj)
    #     conn_id = conn_obj.get("id")
    #     for x in range(10):
    #         conn_obj.new_tag({"id": f"Tag{x}",
    #                             "description": f"Blah Blah {1+x}",
    #                             "value":x*100.0,
    #                             "address": f"PLC_Tag_F{x}",
    #                             })
    #     print(conn_obj)
        for tag_id, tag_obj in conn_obj.get("tags").items():
    #         dm.save_tag(tag_obj)
            dm.subscribe(tag_obj.get('tagname'), 'test', lambda:print())
    print(dm.sub_db.session.query(dm.sub_db.orm).filter(dm.sub_db.orm.connection == 'Fred8').all())
    dm.close_db()