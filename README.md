# brain-plasma
Sharing data between callbacks, on Apache Plasma. Built for Dash, useful anywhere. 

---

`brain-plasma` is a high-level wrapper for the Apache Plasma PlasmaClient API with an added object naming, reference, and changing system.

### Key Features

1. Create and reference named shared-memory Python objects
2. Change the value of those shared-memory Python objects
3. Thread-safe: it doesn't matter if the processes or threads sharing the `Brain` object share memory or not; the namespace is only stored in Plasma and checked each time any object is referenced.
4. Store large objects, especially Pandas and NumPy objects, in the backend
5. Access those objects very quickly - faster than Parquet. Pulling ~10,000,000 row Pandas object takes about .45 seconds, storing it takes about 1 second. Instantaneous for most objects of reasonable size, including Pandas up to ~200,000 rows.

**Current Drawbacks**

4. (3.) above slows the `Brain` down, minutely...but it makes it safe for programs that don't share memory. This only becomes a problem if there are thousands of variables that need to be parsed each time, but even then it's still a small fraction of a second.
6. Limited to Arrow-serializable objects.

### Basic Usage

```
$ pip install pyarrow
$ pip install brain-plasma
$ plasma_store -m 50000000 -s /tmp/plasma & disown
```

```
from brain_plasma import Brain
brain = Brain()

this = 'a text object'
that = [1,2,3,4]
those = pd.DataFrame(dict(this=[1,2,3,4],that=[4,5,6,7]))

brain.learn(this,'this')
brain.learn(that,'that)
brain.learn(those,'those')

brain.recall('this')
> 'a text object'

brain.recall('that')
> [1,2,3,4]

type(brain.recall('those'))
> pandas.core.frame.DataFrame

brain.forget('this')
brain.recall('this')
> # error, that name/object no longer exists

brain.names()
> ['that','those']
```

### Basic API Reference for `brain_plasma.Brain`

**Attributes**

`Brain.client`

The underlying PlasmaClient instance. Created at instantiation. Requires plasma_store to be running locally.

`Brain.path`

The path to the PlasmaClient connection folder. Default is `/tmp/plasma` but can be changed by using `brain = Brain(path='/my/new/path')`

**Methods**

`Brain.learn(thing, name, description=False)`

Store object `thing` in Plasma, reference later with `name`

`Brain.recall(name)`

Get the value of the object with name `name` from Plasma

`Brain.info(name)`

Get the metadata associated with the object with name `name`

`Brain.forget(name)`

Delete the object in Plasma with name `name` as well as the index object

`Brain.names()`

Get a list of all objects that `Brain` knows the name of (all names in the `Brain` namespace)

`Brain.ids()`

Get a list of all the plasma.ObjectIDs that brain knows the name of

`Brain.knowledge()`

Get a list of all the "brain object" index objects used as a reference: name (variable name), name_id (bytes of the ObjectID for the index object) d(bytes of ObjectID for the value), description (False if not assigned)

`Brain.sleep()`

Disconnect `Brain.client` from Plasma. Must use `Brain.wake_up()` to use the `Brain` again.

`Brain.wake_up()`

Reconnect `Brain.client` to Plasma.

`Brain.dead()`

Disconnect `Brain.client` and kill the `plasma_store` process with `$ pkill plasma_store`