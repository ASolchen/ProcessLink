"""
Copyright (c) 2021 Adam Solchenberger asolchenberger@gmail.com, Jason Engman engmanj@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


###from ..process_link import ProcessLink
import time



class ConxManager():
    def __init__(self,process_link: "ProcessLink") -> None:
        ###self.link = ProcessLink()
        self.link = process_link
        self.connections = {}
        self.tag_subs = {}

    def add_con(self, params):
        idx = params.get('id')
        val = self.link.new_connection(params)
        self.connections[idx] = val
        self.tag_subs[idx] = {"tags": [], "connected": False}
        
    def new_tag(self, conx_id, params):
        if not self.connections.get(conx_id):
            raise Exception(f"Fuck! no connection named {conx_id}")
        t = self.connections[conx_id].new_tag(params)
        self.tag_subs[conx_id]['tags'].append({"id": params.get('id'), "obj": t})

    def delete_tag(self,conx_id,tag_id):
        if not self.connections.get(conx_id):
            raise Exception(f"Fuck! no connection named {conx_id}")
        if not self.connections[conx_id].polling:
            raise Exception(f"Can't delete tag while polling {conx_id}")
        for i in range (len(self.tag_subs[conx_id]['tags'])):
            if self.tag_subs[conx_id]['tags'][i]['id'] == tag_id:
                del(self.tag_subs[conx_id]['tags'][i])
        self.link.delete_tag(self.connections[conx_id].tags[tag_id],tag_id,conx_id)
        #####################Need to update polling list or prevent while connection connected

    def connect(self, conx_id):
        if not self.connections.get(conx_id):
            raise Exception(f"Fuck! no connection named {conx_id}")
        #if self.connections[conx_id].polling:
        for sub in self.tag_subs:
            for t in self.tag_subs[sub]['tags']:
                self.link.subscribe(f"[{sub}]{t.get('id')}", sub, latest_only=False)
            self.tag_subs[sub]['connected'] = True
        #else:
        #    print('Connection Failed')

    def disconnect(self, conx_id):
        '''Need to create the unsubscribe method'''

        if not self.connections.get(conx_id):
             raise Exception(f"Fuck! no connection named {conx_id}")
        self.tag_subs[conx_id]['connected'] = False
        for sub in self.tag_subs:
            for t in self.tag_subs[sub]['tags']:
                self.link.unsubscribe(f"[{sub}]{t.get('id')}", sub)
            self.tag_subs[sub]['connected'] = False
    
    def is_polling(self, conx_id):
        'return whether connection is connected and polling data'
        #print('con man is polling',self.connections[conx_id].polling,self.link.is_polling(conx_id))
        if not self.connections.get(conx_id):
            return False
        else:
            return self.link.is_polling(conx_id)

    def is_tag(self, conx_id, tag_id):
        'return whether tag is added to connection'
        res = False
        if not self.tag_subs.get(conx_id):
            raise Exception(f"Fuck! no connection named {conx_id}")
        else:
            for i in range (len(self.tag_subs[conx_id]['tags'])):
                if self.tag_subs[conx_id]['tags'][i]['id'] == tag_id:
                    res = True
        return res
                
    def get_data(self, *args):
        data = {}
        for sub in self.tag_subs:
            if self.tag_subs[sub].get('connected'):
                data[sub] = self.link.get_tag_updates(sub)
        return data


'''con_man = ConxManager()
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
con_man.delete_tag('Fred', "PanelVolts")
print(con_man.get_data())
'''
