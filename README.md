# brain-plasma

`brain-plasma` is a high-level wrapper for the Apache Plasma PlasmaClient API with an added naming and namespacing system. Only supported on Mac/Linux with Python 3.5+.

---

## Basic Use

Basic idea: the brain has a list of names that it has "learned" that are attached to objects in Plasma. Learn, recall, and delete stored objects, call it like a dictionary with bracket notation e.g. `brain['x']` and `del`.

```bash
pip install pyarrow
pip install brain-plasma
plasma_store -m 50000000 -s /tmp/plasma
```

```python
from brain_plasma import Brain
brain = Brain()

this = 'a text object'
those = pd.DataFrame(dict(this=[1,2,3,4],that=[4,5,6,7]))

brain['this'] = this

brain['this']
>>> 'a text object'

brain.names()
>>> ['this']

del brain['this'] # remove the object from the brain's memory
brain['this']
>>> # error, that name/object no longer exists

brain.names()
>>> [] # object is gone
```

**Namespaces**

```python
# change namespace
brain['this'] = 'default text object'
brain.set_namespace('newname')
brain['this'] = 'newname text object'

brain.set_namespace('default')
brain['this']
>>> 'default text object'

brain.names(namespaces='all')
['this','this']
```

## Key Features

1. Create and reference named shared-memory Python objects
2. Change the value of those shared-memory Python objects
3. Thread-safe: it doesn't matter if the processes or threads sharing the `Brain` object share memory or not; the namespace is only stored in Plasma and checked each time any object is referenced.
4. Store large objects, especially Pandas and NumPy objects, in the backend of something
5. Access those objects very quickly - faster even than Parquet. Instantaneous for most objects of any size.
6. Quickly create, switch between, and remove unique namespaces while requiring only one backend instance of `plasma_store`

## Potential use cases

- Access small or large data objects quickly in a data-intensive application
- Keep test values intact while restarting some process over and over again
- Share data between callbacks in Ploty Dash (or any other Python backend)
- Share data between Jupyter Notebooks
- Store and persist user state values in unique namespaces server-side

**Current Drawbacks**

1. Limited to Arrow-serializable objects. This includes Pandas, NumPy, TensorFlow, all built-in Python objects, and many more. However, some things will not be supported. Check before using.

## Release notes

**RELEASE WITH BREAKING CHANGES: `v0.3`**

- totally rewritten Brain client uses hashing to talk to the store more efficiently
- several order of magnitude speedup - seriously
- many fewer client calls
- simpler API
- unit tests for most functions using pytest w/mock PlasmaClient in `brain_plasma.mock`
- maintain Brain from v0.2 for backward compatibility; import with `brain_plasma.compatibility import v02Brain`
- full type hinting
- custom errors for each exception type in `brain_plasma.exceptions`

**RELEASE WITH BREAKING CHANGES: `v0.2`**

- changed parameter order of `learn()` to `('name',thing)` which is more intuitive (but you should always use bracket notation)
- removed ability to start, kill, or resize the underlying brain instance (stability)
- added unique intra-plasma-instance namespaces
- `len(brain)`, `del brain['this']` and `'this' in brain` are now avilable (implemented `__len__`, `__delitem__`, and `__contains__`)

NOTE: stability problems should be resolved in `v0.2`.

## Testing

Test with pytest.

```bash
pip install pytest
git clone https://github.com/russellromney/brain-plasma
cd brain-plasma
pytest
```

---

## API Reference for `brain_plasma.Brain`

**Initialization**

```python
# simple
brain = Brain()
# show defaults
brain = Brain(path: str='/tmp/plasma',namespace: str='default')
```

Parameters:

- `path` - which path to use to connect to the plasma store
- `namespace` - which namespace to use

### Attributes

**`Brain.client`**

The underlying PlasmaClient object. Created at instantiation. Requires plasma_store to be running locally.

**`Brain.path`**

The path to the PlasmaClient connection folder. Default is `/tmp/plasma` but can be changed by using `brain = Brain(path='/my/new/path')`

**`Brain.bytes`**

int - number of bytes in `plasma_store`

**`Brain.mb`**

str - number of mb available, e.g. `'50 MB'`

**`Brain.namespace`**

str - the name of the current namespace

### Methods

#### Interacting with stored objects

Store an item's value and recall the value with bracket notation:

```python
brain['this'] = 5
x = brain['this']
```

i.e. `Brain.__setitem__` and `Brain.__getitem__`

Delete a name and its stored value like `del brain['this']`

#### (Underlying API) - Interacting with stored objects

**`Brain.learn(name, thing, description=False)`**

Store object `thing` in Plasma, reference later with `name`

**`Brain.recall(name)`**

Get the value of the object with name `name` from Plasma

**`Brain.forget(name)`**

Delete the object in Plasma with name `name` as well as the index object

#### Interacting with namespaces (NEW)

Since `v0.2`. Lightweight namespaces within a single `plasma_store` instance. Object names are unique within namespaces but can be duplicated within namespaces. Namespaces can be created and removed at anytime along with all of their objects and names.

> IMPORTANT: Namespaces must be between at least 5 and no more than 15 characters.
> This is because namespace strings are used as the prefix of the plasma.ObjectID for all objects in a given namespace, and must allow enough room for at least 6 unique random characters to ensure ObjectID uniqueness with near certainty. The namespaces set is stored in a unique namespace object with ObjectID as `plasma.ObjectID(b'brain_namespaces_set')`.

**`Brain.set_namespace(namespace=None)`**

Changes `self.namespace` to `namespace` and adds `namespace` to the unique namespace object if it does not already exist. Returns name of namespace if successful. If namespace is not specified, simply returns name of current namespace.

**`Brain.namespaces()`**

Returns set of unique namespaces.

**`Brain.remove_namespace(namespace=None)`**

Removes namespace `namespace` and removes all of the objects in `namespace`. If `namespace` is not specified, it removes the current namespace i.e. self.namespace.

#### Object metadata

**`Brain.object_id(name: str)`**

Get the ObjectId of the value of the name.

**`Brain.object_ids()`**

Get a dictionary of the names and the ObjectIds of the values associated with all the names in the namespace. Allows for more granular work with PlasmaClient.

**`Brain.names(namespace: str="default")`**

Get a list of all objects that `Brain` knows the name of (all names in the specified namespace).

If `namespace='all'`, then it gives the list of all the names in all the available namespaces.

Use `'name' in brain` as a shortcut for checking if a name is known.

**`Brain.ids()`**

Get a list of all the plasma.ObjectID instances that brain knows the name of.

**`Brain.metadata(*names, output='dict')`**

Get a dictionary (or list if `output='list'`) of all the metadata for the names you list.
Returns a single dict if you provide only one name. Otherwise returns a list of metadata
objects or dict of name:metadata pairs.

Get the metadata dict object associated with the object with name `name`.

Metadata object structure:

```
{
    name: str (variable name),
    metadata_id: bytes (bytes of the ObjectID for the index object),
    value_id: bytes (bytes of ObjectID for the value),
    description: str (False if not assigned),
    namespace: str (the object's namespace)
}
```

**Store Metadata**

**`Brain.size()`**

Calls `brain.client.store_capacity()`, returns int - number of bytes available in the plasma_store, e.g. `50000000`

**`Brain.used()`**

Calculates how many bytes the plasma_store is using.

**`Brain.free()`**

Calculates how many bytes of the plasma_store is not used

**Managing connection state**

**`Brain.sleep()`**

Disconnect `Brain.client` from Plasma. Must use `Brain.wake_up()` to use the `Brain` again.

**`Brain.wake_up()`**

Reconnect `Brain.client` to Plasma.

### Exceptions

v0.3 introduces custom exceptions for each type of problem the user may encounter (within limits). Import and use like:

```python
from brain_plasma.exceptions import (
    BrainNameNotExistError,
    BrainNamespaceNameError,
    BrainNamespaceNotExistError,
    BrainNamespaceRemoveDefaultError,
    BrainNameLengthError,
    BrainNameTypeError,
    BrainClientDisconnectedError,
    BrainRemoveOldNameValueError,
    BrainLearnNameError,
    BrainUpdateNameError,
)
```

---

## Notes and TODO

Apache PlasmaClient API reference: https://arrow.apache.org/docs/python/generated/pyarrow.plasma.PlasmaClient.html#pyarrow.plasma.PlasmaClient

Apache Plasma docs: https://arrow.apache.org/docs/python/plasma.html#

**TODO**

- multiple assignment & get
  - this is actually very easy, as the underlying PlasmaClient API already supports this.
  - just need to iterate over names and objects as zip and call the basic learn for each.
- ability to specify namespace for all class methods.
  - this would allow you to do everything declaratively without needing another line of code
  - right now everything uses self.namespace
- do special things optimizing the PlasmaClient interactions with NumPy and Pandas objects
- ability to persist items on disk and recall them with the same API
- specify in docs or with error messages which objects cannot be used due to serialization constraints
- ability to dump all/specific objects and name reference to a declared disk location
  - plus ability to recover these changes later - maybe make it standard behaviour to check the standard location
- brain logging

---

Made with :heart: by Russell Romney in Madison, WI. Thanks for the contributions from @tcbegley (so far)
