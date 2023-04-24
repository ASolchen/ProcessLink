from process_link import ProcessLink
import time



if __name__ != "__main__":
    link = ProcessLink()
    #print(link.get('connection_types'))
    #link.set("db_file", "test.db")
    #link.load_db()
    # for x in range(10):
    c = link.new_connection({"id": f"Fred",
                                 "connection_type": "modbusTCP",
                                 "description": "fred's connection",
                                 "host": '192.168.20.25',
                                 'pollrate': 1,
                                 })
    
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

    #link.save_connection(c)

    tag_obj1 = c.new_tag({"id": f"PanelAmps", "description": f"Blah Blah","value":0,"address": 99 })
    tag_obj2 = c.new_tag({"id": f"PanelVolts", "description": f"Blah Blah","value":0,"address": 149 })
    link.subscribe("[Fred]PanelAmps", "Display01", latest_only=False)    
    link.subscribe("[Fred]PanelVolts", "Display01", latest_only=False)
    #link.save_tag(tag_obj)
    
    
    #link.subscribe("[Fred0]Tag1", "Display01")
    #link.subscribe("[Fred0]Tag0", "Display02", latest_only=False) #get buffered values
    #link.subscribe("[Fred0]Tag1", "Display02")
    time.sleep(1)
    for x in range(10):
        print(link.get_tag_updates("Display01"))
        time.sleep(5)
    #link.close_db()




class ConxManager():
    def __init__(self) -> None:
        self.link = ProcessLink()
        self.connections = {}
        self.tag_subs = {}

    def add_con(self, params):
        idx = params.get('id')
        self.connections[idx] = self.link.new_connection(params)
        self.tag_subs[idx] = {"tags": [], "connected": False}
        
    def new_tag(self, conx_id, params):
        if not self.connections.get(conx_id):
            raise Exception(f"Fuck! no connection named {conx_id}")
        t = self.connections[conx_id].new_tag(params)
        self.tag_subs[conx_id]['tags'].append({"id": params.get('id'), "obj": t})

    def connect(self, conx_id):
        if not self.connections.get(conx_id):
            raise Exception(f"Fuck! no connection named {conx_id}")
        for sub in self.tag_subs:
            for t in self.tag_subs[sub]['tags']:
                self.link.subscribe(f"[{sub}]{t.get('id')}", sub, latest_only=False)
            self.tag_subs[sub]['connected'] = True

    def disconnect(self, conx_id):
        pass        #Need to create the unsubscribe method

        # if not self.connections.get(conx_id):
        #     raise Exception(f"Fuck! no connection named {conx_id}")
        # for sub in self.tag_subs:
        #     for t in self.tag_subs[sub]['tags']:
        #         self.link.unsubscribe(f"[{sub}]{t.get('id')}", sub, latest_only=False)
        #     self.tag_subs[sub]['connected'] = True

    def get_data(self, *args):
        data = {}
        for sub in self.tag_subs:
            if self.tag_subs[sub].get('connected'):
                data[sub] = self.link.get_tag_updates(sub)
        return data


con_man = ConxManager()
con_man.add_con({"id": f"Fred",
                                 "connection_type": "modbusTCP",
                                 "description": "fred's connection",
                                 "host": '192.168.20.25',
                                 'pollrate': 0.1,
                                 })
con_man.new_tag('Fred', {"id": f"PanelAmps", "description": f"Blah Blah","value":0,"address": 99 })
con_man.new_tag('Fred', {"id": f"PanelVolts", "description": f"Blah Blah","value":0,"address": 149 })
con_man.connect('Fred')
for x in range(20):
    print(con_man.get_data())
    time.sleep(0.5)

    

    
        



