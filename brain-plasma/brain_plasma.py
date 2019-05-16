import pyarrow as pa
from pyarrow import plasma
import os
import random
import string
import time

# running shell commands
# https://stackoverflow.com/questions/89228/calling-an-external-command-in-python
# apache plasma documentation
# https://arrow.apache.org/docs/python/plasma.html

class Brain:
    def __init__(
        self,
        # TODO connect=True
        path="/tmp/plasma",
        size=50000000, # 50MB
        start_process=True # start plasma_store ?
    ):
        self.path = path
        self.bytes = size
        self.mb = '{} MB'.format(round(self.bytes/1000000))
        # start plasma_store if necessary
        if start_process:
            # start the plasma_client - if there is one running at that path, shut it down and restart it with the current size
            self.start(size=self.bytes,path=self.path)
            # if not proc.returncode:
            #     raise BaseException('BrainError: could not start plasma_store; here is the error message:\n{}',format(proc.stderr))
        self.client = plasma.connect(self.path)
        if not start_process:
            self.bytes = self.size()

    ### core functions
    def learn(self,thing,name,description=False):
        '''put a given object to the plasma store'''
        # check that name is string
        if not type(name)==str:
            raise BaseException('BrainError: type of name "{}" is not string'.format(name))

        ### get names and brain object
        # get a "names object id"
        this_id,name_id = brain_new_ids_or_existing_ids(name,self.client)
        # if object is normal python object
        brain_object = {
            'name':name,
            'id':this_id.binary(),
            'description':description,
            'name_id':name_id.binary()
        }
        
        # check if name is already taken; if it is, delete its value stored at its ObjectID and delete its brain_object name index
        if brain_name_exists(name,self.client):
            self.forget(name)

        ### store them
        # put the thing to store
        self.client.put(thing,this_id)
        # put the name id reference to store
        self.client.put(brain_object,name_id)
        
        ### TODO - make specific things for pandas and numpy
        # special if object is pandas
        # special if object is numpy 

    def __setitem__(self, name, item):
        self.learn(item, name)

    def recall(self,name):
        '''get an object value based on its Brain name'''
        names_ = brain_names_ids(self.client)
        this_id = names_[name]
        brain_name_error(name,self.client)        
        return self.client.get(this_id,timeout_ms=100)
    
    def __getitem__(self, name):
        return self.recall(name)

    def info(self,name):
        '''return metadata object based on its Brain name'''
        names_ = brain_names_objects(self.client)
        for x in names_:
            if x['name']==name:
                brain_object = x
        return brain_object
    
    def forget(self,name):
        '''delete an object based on its Brain name'''
        names_ = brain_names_ids(self.client)
        brain_object = brain_names_objects(self.client)
        for x in brain_object:
            if x['name']==name:
                brain_object=x
                break
        this_id = names_[name]
        name_id = plasma.ObjectID(brain_object['name_id'])
        self.client.delete([this_id,name_id])
    
    def names(self):
        '''return a dictionary of the names that brain knows'''
        names_ = brain_names_objects(self.client)
        return [x['name'] for x in names_]

    def ids(self):
        '''return list of Object IDs the brain knows that are attached to names'''
        names_ = brain_names_objects(self.client)
        return [plasma.ObjectID(x['id']) for x in names_]

    def knowledge(self):
        '''return a dictionary of all names and ids that brain knows'''
        return brain_names_objects(self.client)
        
    def start(self,path=None,size=None):
        '''start plasma_store again if it is dead'''
        try:
            # test to see if the client is connected to a plasma_store
            temp = self.client.put(5)
            self.client.delete(temp)
            self.bytes = self.size()
            self.mb = '{} MB'.format(round(self.bytes/1000000))
        except:
            # if not, start the store and connect it
            if size!=None:
                self.bytes = size
            if path!=None:
                self.path = path
            # check if there is an existing plasma_store at that place; if so, connect to it and resize it to the new sie
            try:
                self.client = plasma.connect(self.path)
                for name in self.names():
                    self.forget(name)
                if size!=None:
                    self.resize(size)                
            except:
                os.system('plasma_store -m {} -s {} & disown'.format(self.bytes,self.path))
                self.wake_up()
            
    def dead(self,i_am_sure=True):
        '''pkill the plasma_store processes'''
        if i_am_sure:
            self.client.disconnect()
            # kill the oldest process
            # when using self.resize(), this is the original plasma_store, then is the temp_brain plasma_store
            os.system('pkill -o plasma_store')
        else:
            return 'You were not sure about brain.dead(). I did not kill the brain.'
    
    def sleep(self):
        '''disconnect from the client'''
        self.client.disconnect()

    def wake_up(self):
        '''reconnect to the client'''
        self.client = plasma.connect(self.path)
        time.sleep(.1)
        self.bytes = self.size()
        self.mb = '{} MB'.format(round(self.bytes/1000000))

    def size(self):
        '''show the available bytes of the underlying plasma_store; wrapper for PlasmaClient.store_capacity()'''
        try:
            temp = self.client.put(5)
            self.client.delete([temp])
        except:
            raise BaseException
        self.bytes = self.client.store_capacity()
        self.mb = '{} MB'.format(round(self.bytes/1000000))
        return self.bytes

    def resize(self,size):
        # TODO - add a way to deal with desriptions
        # create temp brain with old size by temp path
        self.temp_brain = Brain(size=self.bytes,path='/tmp/plasma-temp',start_process=True)
        # save the value of each name in the temp brain
        for name in self.names():
            self.temp_brain[name] = self.recall(name)
        # delete the old brain's plasma_store
        self.dead()
        # create new brain with same path as old brain, but new size
          # this also resets self.bytes to input size (see self.start())
        self.start(size=size)
        # for each name in the temp brain, save that name & value to the resized brain
        for name in self.temp_brain.names():
            self.learn(self.temp_brain.recall(name),name)
        self.temp_brain.dead(i_am_sure=True)
        del self.temp_brain

    def object_map(self):
        '''return a dictionary of names and their associated ObjectIDs'''
        return brain_names_ids(self.client)

    def used(self):
        '''get the total used bytes in the underlying plasma_store'''
        total = 0
        l = self.client.list()
        for x in l.keys():
            total+=l[x]['data_size']+l[x]['metadata_size']
        return total
    
    def free(self):
        '''get the total unused bytes in the underlying plasma_store'''
        return self.size() - self.used()



    # def brain_size(self):
    #     '''return the total memory allocated to the store'''
    #     return self.client.store_capacity()









### utility functions - not callable outside the function
def brain_ids(client):
    '''return a list of ALL ObjectIDs the brain knows'''
    return list(client.list().keys())

def brain_new_ids_or_existing_ids(name,client):
    '''if name exists, returns object id of that name and that client; else new ids'''
    if brain_name_exists(name,client):
        # get the brain_object for the old name
        brain_object = brain_names_objects(client)
        for x in brain_object:
            if x['name']==name:
                brain_object=x
                break
        # delete the old name and thing objects
        client.delete([plasma.ObjectID(brain_object['name_id']),plasma.ObjectID(brain_object['id'])])
        # get the new ids
        thing_id = plasma.ObjectID(brain_object['id'])
        name_id = plasma.ObjectID(brain_object['name_id'])
    else:
        # create a new name id and thing id
        name_id = brain_create_named_object(name)
        thing_id = plasma.ObjectID.from_random()
    return thing_id,name_id

def brain_names_ids(client):
    '''get dict of names and ObjectIDs in the store'''
    names_ = brain_names_objects(client)
    return {x['name']:plasma.ObjectID(x['id']) for x in names_}

def brain_names_objects(client):
    '''
    get a dictionary of names and ids that brain knows

    note on this: 
        - the convention is that every name that brain knows is
        stored separately as an object in the plasma store with 
        the prefix b"names"...
        - so brain gets the names by 
            - getting all object ids
            - any that have b'names' in them will be dictionaries of 
            type (numpy, general, pandas) and object id, or other metadata
    '''
    # get all ids
    all_ids = brain_ids(client)
    # get all ids that contain names
    known_ids = []
    for x in all_ids:
        # '6e616d657' is "name" in bytes
        if '6e616d657' in str(x).lower():
            known_ids.append(x)
    # get all actual objects (names and type) with those ids
    values = client.get(known_ids,timeout_ms=100)
    
    return values

def brain_name_exists(name,client):
    '''confirm that the plasma ObjectID for a given name'''
    names_ = brain_names_ids(client)
    return (name in names_.keys())

def brain_name_error(name,client):
    '''raise error if the name does not exist'''
    if not brain_name_exists(name,client):
        raise BaseException('BrainError: Brain does not know the name "{}"'.format(name))

def brain_create_named_object(name):
    '''return a random ObjectID that has "names" in it'''
    letters = string.ascii_letters
    random_letters = ''.join(random.choice(letters) for i in range(15))
    return plasma.ObjectID(bytes('names'+random_letters,'utf-8'))