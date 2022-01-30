from process_link import ProcessLink
import time



if __name__ == "__main__":
    link = ProcessLink()
    #print(link.get('connection_types'))
    link.set("db_file", "test.db")
    link.load_db()
    # for x in range(10):
    #     c = link.new_connection({"id": f"Fred{x}",
    #                             "connection_type": "logix",
    #                             "description": "fred's connection",
    #                             "host": '192.168.1.169'
    #                             })
    
    #print(link.get("connections")) # one that works
    # #print(link.get("bad_prop")) # one that fails
    #for conn_id, conn_obj in link.get("connections").items():
    #     link.save_connection(conn_obj)
    #     print(conn_obj)
    #     conn_id = conn_obj.get("id")
    #     for x in range(10):
    #         conn_obj.new_tag({"id": f"Tag{x}",
    #                             "description": f"Blah Blah {1+x}",
    #                             "value":x*100.0,
    #                             "address": f"PLC_Tag_F{x}",
    #                             })
    #     print(conn_obj)
    #    for tag_id, tag_obj in conn_obj.get("tags").items():
    #         link.save_tag(tag_obj)
    #       link.subscribe(tag_obj.get('tagname'), 'test', update_cb)
    #print(link.sub_db.session.query(link.sub_db.orm).filter(link.sub_db.orm.connection == 'Fred8').all())
    link.subscribe("[Fred0]Tag0", "Display01")
    link.subscribe("[Fred0]Tag1", "Display01")
    time.sleep(1)
    for x in range(10):
        link.get_tag_updates("Display01")
        time.sleep(1)
    link.close_db()