from brain_plasma import Brain
import json
import pandas as pd
import numpy as np
import os
import time

start = time.time()

# create a Brain with a store running
print('starting plasma_state')

# start the plasma_store at 
path = '/tmp/brain_plasma_test'
brain = Brain(path=path)

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
    brain.learn(name,thing)

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
brain['l'] = l

# create large pandas object with standard types
m = pd.DataFrame(dict(this=[i for i in range(100000)],that=[i for i in range(100000)][::-1]))
brain['m'] = m

# create pandas object with nonstandard types
n = pd.DataFrame(dict(this=[a,b,c,d,e,f,g,h,i,j,k]))
brain['n'] = n

# create pandas series
o = pd.Series([1,2,3,4,5,6,])
brain['o'] = o

# read all pandas objects
assert list(l.columns)==list(brain.recall('l').columns)
assert [x for x in l.that]==[x for x in brain.recall('l').that]
assert list(m.this[:5].values)==list(brain.recall('m').this[:5].values)
assert n.values[0]==brain.recall('n').values[0]
assert o.values[0]==brain.recall('o').values[0]

# create numpy objects
p = np.array([1,2,3,4,5])
q = np.mean
brain['p'] = p
brain['q'] = q

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
brain.learn('a',l)
brain.learn('b',m)
brain.learn('c',n)
brain.learn('d',o)
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
    x = 0
    try:
        brain.recall(name)
        x = 5
    except:
        pass
    if x==5:
        raise BaseException

# sleep the brain
brain.sleep()

# reconnect the brain
brain.wake_up()




## namespaces
print('testing namespaces')
# current namespace is 'default'
assert brain.show_namespaces()=={'default'}

# add value to default namespace
brain['a'] = 5

# value has namespace value in its brain_object
assert brain.info('a')['namespace']=='default'

# add a new namespace - name too short
x = True
try:
    brain.set_namespace('new')
    x = False
except: pass
assert x

# add new namespace - name too long
try:
    brain.set_namespace('12345678901234567890')
    x = False
except: pass
assert x

# add new namespace works
try: brain.set_namespace('default')
except:
    x = False
assert x

# removing default namespace fails
try:
    brain.remove_namespace('default')
    x = False
except: pass
assert x

# add new namespace (normal)
brain.set_namespace('newname')
assert brain.namespace=='newname'
assert brain.show_namespaces()=={'default','newname'}

# namespace names don't overlap
assert brain.names()==[]

# can see all names in all namespaces
assert brain.names(namespace='all') != []

# can add value with same name without problem 
brain['a'] = 6

# not pulling value from default namespace
assert brain['a'] != 5

# value in new namespace has new namespace in its brain_object
assert brain.info('a')['namespace']=='newname'

# can remove value in current namespace
brain.forget('a')
try: 
    print(brain['a'])
    x=False
except: pass
assert x

# removing value in new namespace does not affect other namespace
brain.set_namespace('default')
assert brain['a']==5

# can remove other namespace from any other namespace, or default;
# removing a namespace from that namespace switches it to default
brain.set_namespace('joyful')
brain.set_namespace('donuts')
brain.remove_namespace('joyful')
assert brain.namespace=='donuts'
brain.remove_namespace('donuts')
assert brain.namespace=='default'
brain.remove_namespace('newname')
assert brain.show_namespaces()=={'default'}

# removing a namespace removes its values
assert len(brain.names(namespace='all'))==1

for name in brain.names(namespace='all'):
    brain.forget(name)

print('all brain-plasma tests passed!')
print('time:',time.time()-start)