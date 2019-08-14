from brain_plasma.brain_plasma import Brain
import pickle
import random
import os

PERSIST_PATH = 'persist-data/'

description='''
the idea is basically caching data in plasma store and saving it on disk

the idea has two parts:
    - pulling data from the plasma store is very fast after the first time
    - saving data to the plasma store is slow
    - each time data changes, brain pickles it to disk
    - if the data does not exist in brain, brain pulls pulls it from disk and learns it before returning it

main benefits
    - basically saves application state for future use
    - plasma_store doesn't need to be up all the time
    - brain can function even without a plasma store running (maybe with a 'persist_only' option or something)


structure of the persist folder
    - all things in a single portable file
    - three types
        - changes.db
        - pickled brain objects with clear names
        - randomly named object values
    - advantages:
        - no need to loop through files
        - only read/edit two files, the brain object and the value file, both by name directly

<root>/
    log.db
    namespace_<namespace>_value_<name>
    fq98wyrhoiasudhf
    wyeroiuashfluhaa

each non-folder object is a pickled dictionary like
{
    'name':'var1',
    'namespace':'default',
    'brain-object':{
        ...
    },
    'value':[some,value]
}

persisting process:
- like brain.learn
- brain object differences:
    - store
        - contains persist boolean
        - contains persist timestamp
    - disk
        - contains persist timestamp
        - contains persist boolean
        - contains filepath to pickled object

- in try/except
- before try/except:
    - recall previous value and save to temp
- try to:
    - create new brain object
    - if self._brain_name_exists
        - delete from store
    - put the new thing to the store and put the new brain object to the store i.e. normal brain.learn
    - put the new brain object to 
- except:
    - 
'''

class PersistBrain(Brain):
    '''
    a test brain that saves both to plasma and to a folder in its persist path
    '''
    def __init__(self,persist_path='brain-persist/',**kwargs):
        super().__init__(**kwargs)
        assert open(persist_path+'test','wb'), 'PersistBrain Error: unable to access persist_path "{}"'.format(persist_path)
        self.persist_path = persist_path

    def persist_learn(self,name,thing,description=False):
        '''put the item in the store and persist on disk'''
        if not type(name)==str:
            raise BaseException('PersistBrain Error: type of name "{}" is not string'.format(name))

        this_id,name_id = self._brain_new_ids_or_existing_ids(name,self.client)
        brain_object = {
            'name':name,
            'id':this_id.binary(),
            'description':description,
            'name_id':name_id.binary(),
            'namespace':self.namespace,
            'value_persist_path':self.persist_path+self._random_letters(),
            'brain_object_persist_path':self._persist_brain_object_path(name)
        }

        # check if name is already taken; if it is, delete its value stored at its ObjectID and delete its brain_object name index
        if self._brain_name_exists(name,self.client):
            self.forget(name)
            self.persist_forget(name)
        
        # save to store
        self.learn(name,thing)
        # save brain object to disk
        pickle.dump(brain_object,open(brain_object['brain_object_persist_path'],'wb'))
        # save value to disk
        value_ = {name:thing}
        pickle.dump(value_,open(brain_object['value_persist_path'],'wb'))

    def __setitem__(self,name,item):
        self.learn(name,item)
        self.persist_learn(name,item)

    def persist_recall(self,name):
        value_path = pickle.load(open(self._persist_brain_object_path(name),'rb'))['value_persist_path']
        return pickle.load(open(value_path,'rb'))[name]
    
    def __getitem__(self,name):
        try:
            if name in self.names():
                return self.recall(name)
        except:
            pass

        print(self.persist_names())
        if name in self.persist_names():
            # save it to the store
            val_ = self.persist_recall(name)
            self.learn(name,val_)
            return val_
        print(self.persist_names())
        raise KeyError(name)

    def persist_forget(self,name):
        '''delete an object from disk based on its brain name'''
        ref = self._persist_brain_object(name)
        value_path = ref['value_persist_path']
        # remove the object itself
        os.remove(value_path)
        # remove the brain object
        os.remove(ref['brain_object_persist_path'])

    def __delitem__(self,name):
        self.forget(name)
        self.persist_forget(name)

    def persist_names(self,namespace=None):
        '''return names all known variables on disk'''
        all_names = [x for x in os.listdir(self.persist_path) if 'namespace' in x]
        if namespace is None:
            namespace = self.namespace
        if namespace=='all':
            return [x.split('name_')[-1] for x in all_names]
        else:
            return [x.split('namespace_{}_name_'.format(namespace))[-1] for x in all_names if 'namespace_{}_name_'.format(namespace) in x]

    def _random_letters(self):
        return ''.join([random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') for x in range(20)])

    def _persist_brain_object_path(self,name):
        return self.persist_path+'namespace_{}_name_{}'.format(self.namespace,name)

    def _persist_brain_object(self,name):
        return pickle.load(open(self._persist_brain_object_path(name),'rb'))

    def _persist_name_error(self,name):
        '''raise error if the name does not exist'''
        if not name in self.persist_names():
            raise BaseException('PersistBrain Error: PersistBrain does not know the name "{}" in namespace "{}"'.format(name,self.namespace))
    
