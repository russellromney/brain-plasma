import time
import uuid

from brain_plasma.v02compatibility import Brain as v02Brain
from brain_plasma import Brain

brain = v02Brain(path="/tmp/brain")
hbrain = Brain(path="/tmp/hash-brain")
few = 10
many = 100
times = {few: {"old":{},'hash':{}}, many: {"old":{},'hash':{}}}

#################################################################
# few things
#################################################################
# make a few things
ids = [uuid.uuid1().hex for x in range(few)]

start = time.time()
sets = [brain.learn(ID, ID) for ID in ids]
times[few]['old']['learn'] = time.time()-start

start = time.time()
hsets = [hbrain.learn(ID, ID) for ID in ids]
times[few]['hash']['learn'] = time.time()-start

# test speed with a few things
start = time.time()
outs = [brain[ID] for ID in ids]
times[few]["old"]['recall'] = time.time() - start
start = time.time()
houts = [hbrain[ID] for ID in ids]
times[few]["hash"]['recall'] = time.time() - start

#################################################################
# many things
#################################################################
ids = [uuid.uuid1().hex for x in range(many)]
start = time.time()
sets = [brain.learn(ID, ID) for ID in ids]
times[many]['old']['learn'] = time.time()-start
start = time.time()
hsets = [hbrain.learn(ID, ID) for ID in ids]
times[many]['hash']['learn'] = time.time()-start

# test speed with a few things
start = time.time()
outs = [brain[ID] for ID in ids]
times[many]["old"]['recall'] = time.time() - start
start = time.time()
houts = [hbrain[ID] for ID in ids]
times[many]["hash"]['recall'] = time.time() - start

s = f"""
{many} items:
    learn:
        old: {times[many]['old']['learn']}
        hash: {times[many]['hash']['learn']}
    recall:
        old: {times[many]['old']['recall']}
        hash: {times[many]['hash']['recall']}
 {few} items:
    learn:
        old: {times[few]['old']['learn']}
        hash: {times[few]['hash']['learn']}
    recall:
        old: {times[few]['old']['recall']}
        hash: {times[few]['hash']['recall']}
"""

print(s)
