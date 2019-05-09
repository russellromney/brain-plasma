from brain_plasma import Brain
import json
import pandas as pd
import numpy as np
import os

# create a Brain with no store running
brain = Brain(start_process=True)

# shut down the Brain
brain.dead(i_am_sure=True)

# create a Brain with a store running
print('starting plasma_state')
os.system('plasma_store -m {} -s {} & disown'.format(50000000,'/tmp/plasma'))
brain = Brain()
brain.dead(i_am_sure=True)

# create a store of nonstandard size
brain = Brain(start_process=True, size=75000000)

# create python objects
a = dict(this=[1,2,3,4],that=[2,3,4,5])
b = 'this is a string'
c = b'this is a byte string'
d = ['this','is','a','list']
e = json.dumps(a)
f = set([1,2,3,4,5])
g = None
h = (3,4,5)
def i(name):
    print(name)
j = 5
k = str
for thing,name in zip([a,b,c,d,e,f,g,h,i,j,k],[i for i in 'abcdefghijk']):
    brain.learn(thing,name)

# read python object
assert a==brain.recall('a')
assert b==brain.recall('b')
assert c==brain.recall('c')
assert d==brain.recall('d')
assert e==brain.recall('e')
assert f==brain.recall('f')
assert g==brain.recall('g')
assert h==brain.recall('h')
assert i('this')==brain.recall('i')('this')
assert j==brain.recall('j')
assert k==brain.recall('k')

# create small pandas object with standard types
l = pd.DataFrame(a)
brain.learn(l,'l')

# create large pandas object with standard types
m = pd.DataFrame(dict(this=[i for i in range(100000)],that=[i for i in range(100000)][::-1]))
brain.learn(m,'m')

# create pandas object with nonstandard types
n = pd.DataFrame(dict(this=[a,b,c,d,e,f,g,h,i,j,k]))
brain.learn(n,'n')

# create pandas series
o = pd.Series([1,2,3,4,5,6,])
brain.learn(o,'o')

# read all pandas objects
assert list(l.columns)==list(brain.recall('l').columns)
assert [x for x in l.that]==[x for x in brain.recall('l').that]
assert list(m.this[:5].values)==list(brain.recall('m').this[:5].values)
assert n.values[0]==brain.recall('n').values[0]
assert o.values[0]==brain.recall('o').values[0]

# create numpy objects
p = np.array([1,2,3,4,5])
q = np.mean
brain.learn(p,'p')
brain.learn(q,'q')

# read list of names
assert set(brain.names())==set([thing for thing in 'abcdefghijklmnopq'])

# read dictionary of names:ObjectIDs
temp = brain.knowledge()
print(temp)

# get metadata about objects
for name in 'abcdefghijklmnopq':
    print(brain.info(name))

# change value of python objects
a = 'this'
b = 'that'
c = 57
d = m
brain.learn(l,'a')
brain.learn(m,'b')
brain.learn(n,'c')
brain.learn(o,'d')
assert list(l.columns)==list(brain.recall('a').columns)
assert set([x for x in l.that])==set([x for x in brain.recall('a').that])
assert list(m.this[:5].values)==list(brain.recall('b').this[:5].values)
assert n.values[0]==brain.recall('c').values[0]
assert o.values[0]==brain.recall('d').values[0]

# changing name values did not change number of names present in plasma
assert len(brain.names())==17

# delete python objects, and confirm that they no longer exist in plasma store
for name in 'abcdefghijklmnopq':
    brain.forget(name)
    try:
        brain.recall(name)
        raise BaseException        
    except:
        pass

# sleep the brain
brain.sleep()

# reconnect the brain
brain.wake_up()

# kill the brain
brain.dead(i_am_sure=True)

# make sure that the underlying process doesn't work
try:
    brain = Brain()
except:
    pass

# start the brain after killing plasma_store; use new size, and path
brain.start(size=75000000,path='/tmp/plasma1')
assert brain.size==75000000

print('dash-brain tests passed!')