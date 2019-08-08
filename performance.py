from brain_plasma import Brain
import numpy as np
import pandas as pd
import pickle
import os
import time

# utilities
def show(t,task,method,code_string,n):
    print(' || '.join([str(x) for x in [round((time.time()-t)*1000,4), task, method, n, code_string]]))

s = '--------'
mem,pic,bra = ('in-memory','pickle','brain-plasma')
brain = Brain() # need at least 2GB
track = pd.DataFrame(columns=['task','method','time','round','code'])
i = 0


print(s,'\nSaving large objects - 10,000,000x10 DataFrame of integers',s)
task = 'save large'

method = mem
code = 'x = 5'
for n in [1,2,3]:
    start = time.time()
    x = 5
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1


method = pic
code = "pickle.dump(pd.DataFrame({a:range(10000000) for a in 'abcdefghij'}),open('test.pkl','wb'))"
for n in [1,2,3]:
    start = time.time()
    pickle.dump(pd.DataFrame({a:range(10000000) for a in 'abcdefghij'}),open('test.pkl','wb'))
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1


method = bra
code = "brain['x'] = pd.DataFrame({a:range(10000000) for a in 'abcdefghij'})"
for n in (1,2,3):
    start = time.time()
    brain['x'] = pd.DataFrame({a:range(10000000) for a in 'abcdefghij'})
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1


print(s,'\nLoading large objects - 10,000,000x10 DataFrame of integers',s)
task = 'load large'

method = mem
code = 'y = x'
for n in [1,2,3]:
    start = time.time()
    y = x
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = pic
code = "y = pickle.load(open('test.pkl','rb'))"
for n in [1,2,3]:
    start = time.time()
    y = pickle.load(open('test.pkl','rb'))
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = bra
code = "y = brain['x']"
for n in (1,2,3):
    start = time.time()
    y = brain['x']
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1










print(s,'\nSaving small objects - a single string "this is the test string"',s)
brain.forget('x')
os.remove('test.pkl')
task = 'save small'

method = mem
code = 'x = "this is the test string"'
for n in [1,2,3]:
    start = time.time()
    x = "this is the test string"
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = pic
code = "pickle.dump('this is the test string',open('test.pkl','wb'))"
for n in [1,2,3]:
    start = time.time()
    pickle.dump('this is the test string',open('test.pkl','wb'))
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = bra
code = "brain['x'] = pd.DataFrame({a:range(10000000) for a in 'abcdefghij'})"
for n in (1,2,3):
    start = time.time()
    brain['x'] = pd.DataFrame({a:range(10000000) for a in 'abcdefghij'})
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1


print(s,'\nLoading small objects - a single string "this is the test string"',s)
task = 'load small'

method = mem
code = 'y = x'
for n in [1,2,3]:
    start = time.time()
    y = x
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = pic
code = "y = pickle.load(open('test.pkl','rb'))"
for n in [1,2,3]:
    start = time.time()
    y = pickle.load(open('test.pkl','rb'))
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = bra
code = "y = brain['x']"
for n in (1,2,3):
    start = time.time()
    y = brain['x']
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1









medium_json = {
    k:{
      "_id": "5d4c5b531211cb59f55e0562",
      "index": 0,
      "guid": "855bad44-23f2-4353-acfe-d8e24fed1486",
      "isActive": True,
      "balance": "$3,840.16",
      "picture": "http://placehold.it/32x32",
      "age": 21,
      "eyeColor": "green",
      "name": {
        "first": "Mcgowan",
        "last": "Conway"
      },
      "company": "AQUACINE",
      "email": "mcgowan.conway@aquacine.us",
      "phone": "+1 (999) 447-2069",
      "address": "655 Boynton Place, Nutrioso, Florida, 9478",
      "about": "Consequat do exercitation incididunt irure sit dolor aliquip amet sunt qui quis fugiat cillum. Aliquip irure enim ullamco tempor ullamco consectetur adipisicing deserunt aliqua tempor exercitation. Aliquip occaecat sit nisi Lorem. Magna incididunt dolor fugiat aliquip commodo eiusmod elit ea occaecat elit elit veniam. Consequat tempor aliquip voluptate sunt exercitation sit adipisicing. Anim consequat officia dolor veniam aliquip voluptate proident tempor Lorem quis nisi dolore.",
      "registered": "Monday, October 6, 2014 12:32 PM",
      "latitude": "65.583401",
      "longitude": "-61.690822",
      "tags": [
        "voluptate",
        "ipsum",
        "exercitation",
        "tempor",
        "est"
      ],
      "range": [
        0,1,2,3,4,5,6,7,8,9
      ],
      "friends": [
        {
          "id": 0,
          "name": "Michele Thompson"
        },
        {
          "id": 1,
          "name": "Ebony Montgomery"
        },
        {
          "id": 2,
          "name": "Wolf Alvarez"
        }
      ],
      "greeting": "Hello, Mcgowan! You have 5 unread messages.",
      "favoriteFruit": "apple"
    }
  for k in range(10)
}
print(s,'\nSaving medium objects - list of 10000 json dictionaries w/random key:values from https://next.json-generator.com/41mO9BHXD',s)
brain.forget('x')
os.remove('test.pkl')
task = 'save medium'

method = mem
code = 'x = [medium_json for x in range(1000)]'
for n in [1,2,3]:
    start = time.time()
    x = [medium_json for x in range(1000)]
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = pic
code = "pickle.dump([medium_json for x in range(1000)],open('test.pkl','wb'))"
for n in [1,2,3]:
    start = time.time()
    pickle.dump([medium_json for x in range(1000)],open('test.pkl','wb'))
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = bra
code = "brain['x'] = [medium_json for x in range(1000)]"
for n in (1,2,3):
    start = time.time()
    brain['x'] = [medium_json for x in range(1000)]
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1


print(s,'\nLoading medium objects - a single string "this is the test string"',s)
task = 'load medium'

method = mem
code = 'y = x'
for n in [1,2,3]:
    start = time.time()
    y = x
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = pic
code = "y = pickle.load(open('test.pkl','rb'))"
for n in [1,2,3]:
    start = time.time()
    y = pickle.load(open('test.pkl','rb'))
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1

method = bra
code = "y = brain['x']"
for n in (1,2,3):
    start = time.time()
    y = brain['x']
    show(start,task,method,code,n)
    track.loc[i] = [task,method,round((time.time()-start)*1000,4),n,code]
    i+=1
