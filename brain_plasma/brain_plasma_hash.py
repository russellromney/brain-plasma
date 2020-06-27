import traceback
from typing import ByteString
import hashlib
from pyarrow import plasma
import os
import random
import string
import time
from .exceptions import (
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

# apache plasma documentation
# https://arrow.apache.org/docs/python/plasma.html


class Brain:
    def __init__(self, namespace="default", path="/tmp/plasma"):
        self.path = path
        self.namespace = namespace
        self.client = plasma.connect(self.path, num_retries=5)
        self.bytes = self.size()
        self.mb = "{} MB".format(round(self.bytes / 1000000))
        self.set_namespace(namespace)

    ### core functions
    def learn(self, name: str, thing: str, description: str = None):
        """
        put a given object to the plasma store
        
        if the name exists already:
            if a new description is provided, uses it; else uses old description
            stores the new value to a new ID
            stores the updated metadata to the same metadata ID
            deletes the old value at the old ID

        Errors:
            BrainNameTypeError
            BrainRemoveOldNameValueError
            BrainLearnNameError
            BrainUpdateNameError
        """
        # CHECK THAT NAME IS STRING
        if not type(name) == str:
            raise BrainNameTypeError(
                f'Type of name "{name}" must be str, not {type(name)}'
            )

        name_exists = self.exists(name)

        ### GET NAMES AND METADATA OBJECT
        metadata_id = self._name_to_namespace_hash(name)
        if name_exists:
            old_metadata = self.client.get(metadata_id)
            old_value_hash = old_metadata["value_id"]
            old_value_id = plasma.ObjectID(old_value_hash)
            description = description or old_metadata["description"]
        value_id = plasma.ObjectID.from_random()

        # CREATE METADATA OBJECT
        metadata = {
            "name": name,
            "value_id": value_id.binary(),
            "description": description or "",
            "metadata_id": metadata_id.binary(),
            "namespace": self.namespace,
        }

        if name_exists:
            # IF NAME EXISTS ALREADY,
            #    STORE THE NEW VALUE AT A NEW LOCATION
            #    DELETE ITS VALUE STORED AT ITS OBJECTID
            #    DELETE ITS BRAIN_OBJECT NAME INDEX
            # (1)
            try:
                self.client.put(thing, value_id)
            # IF THERE'S AN ERROR, JUST STOP
            except:
                traceback.print_exc()
                raise BrainUpdateNameError(
                    f"Unable to update value with name: {name}. Rolled back"
                )

            # (2)
            # REPLACE THE METADATA OBJECT
            self.client.delete([metadata_id])
            self.client.put(metadata, metadata_id)

            # (3)
            # TRY TO DELETE THE OLD NAME
            try:
                self.client.delete([old_value_id])
            # TELL THE USER WHAT WENT WRONG IF THAT DIDN'T WORK
            except:
                traceback.print_exc()
                raise BrainRemoveOldNameValueError(
                    f"Unable to remove old value for name {name} at {old_value_id}"
                )

        else:
            # STORE THE VALUE AND METADATA - IT'S NEW!
            try:
                self.client.put(thing, value_id)
                self.client.put(metadata, metadata_id)
            # IF SOMETHING GOES WRONG, CLEAR UP
            except:
                traceback.print_exc()
                self.client.delete([value_id, metadata_id])
                raise BrainLearnNameError(
                    f"Unable to set value with name: {name}. Rolled back"
                )

    def __setitem__(self, name, item):
        self.learn(name, item)

    def recall(self, name):
        """
        get an object value based on its Brain name
        
        Errors:
            BrainNameNotExistError
        """
        if not self.exists(name):
            raise BrainNameNotExistError(f"Name {name} does not exist.")

        metadata_id = self._name_to_namespace_hash(name)
        metadata = self.client.get(metadata_id, timeout_ms=100)

        value_hash = metadata["value_id"]
        value_id = plasma.ObjectID(value_hash)
        return self.client.get(value_id, timeout_ms=100)

    def exists(self, name: str):
        """
        confirm that the plasma ObjectID for a given name
        """
        id_hash = self._name_to_namespace_hash(name)
        return self.client.contains(id_hash)

    def __getitem__(self, name):
        return self.recall(name)

    def __delitem__(self, name):
        return self.forget(name)

    def __contains__(self, name):
        return name in self.names()

    def __len__(self):
        return len(self.names())

    def info(self, name):
        """return metadata object based on its Brain name"""
        names_ = self._brain_names_objects(self.client)
        for x in names_:
            if x["name"] == name:
                brain_object = x
        return brain_object

    def forget(self, name: str):
        """
        delete an object based on its name
        also deletes the metadata object associated with the name
        does it in a transactional way

        if the name does not exist, doesn't do anything
        """
        if not self.exists(name):
            pass
        else:
            metadata_id = self._name_to_namespace_hash(name)
            metadata = self.client.get(metadata_id, timeout_ms=100)

            value_hash = metadata["value_id"]
            value_id = plasma.ObjectID(value_hash)

            self.client.delete([metadata_id, value_id])

    def names(self, namespace=None):
        """return a list of the names that brain knows, in all namespaces"""
        current_namespace = self.namespace
        if namespace is None:
            namespace = self.namespace
        names = []
        if namespace == "all":
            # for each namespace, add the name objects to the list of names
            for namespace in self.show_namespaces():
                self.namespace = namespace
                names.extend(
                    [x["name"] for x in self._brain_names_objects(self.client)]
                )
        else:
            # return all the names and object_ids in that namespace only
            names = [
                x["name"]
                for x in self._brain_names_objects(self.client)
                if x["namespace"] == self.namespace
            ]

        self.namespace = current_namespace
        return names

    def ids(self):
        """return list of Object IDs the brain knows that are attached to names"""
        names_ = self._brain_names_objects(self.client)
        return [plasma.ObjectID(x["id"]) for x in names_]

    def knowledge(self):
        """return a dictionary of all names and ids that brain knows"""
        return self._brain_names_objects(self.client)

    def sleep(self):
        """disconnect from the client"""
        self.client.disconnect()

    def wake_up(self):
        """reconnect to the client"""
        self.client = plasma.connect(self.path)
        time.sleep(0.2)
        self.bytes = self.size()
        self.mb = "{} MB".format(round(self.bytes / 1000000))

    def size(self):
        """
        show the available bytes of the underlying plasma_store; 
        wrapper for PlasmaClient.store_capacity()

        Errors:
            BrainClientDisconnectedError
        """
        try:
            temp = self.client.put(5)
            self.client.delete([temp])
        except:
            raise BrainClientDisconnectedError
        self.bytes = self.client.store_capacity()
        self.mb = "{} MB".format(round(self.bytes / 1000000))
        return self.bytes

    def object_map(self):
        """return a dictionary of names and their associated ObjectIDs"""
        return self._brain_names_ids()

    def used(self):
        """get the total used bytes in the underlying plasma_store"""
        total = 0
        l = self.client.list()
        for x in l.keys():
            total += l[x]["data_size"] + l[x]["metadata_size"]
        return total

    def free(self):
        """get the total unused bytes in the underlying plasma_store"""
        return self.size() - self.used()

    def set_namespace(self, namespace=None):
        """either return the current namespace or change the current namespace to something new"""
        if namespace is None:
            return self.namespace
        # must be at least four characters and fewer than 15
        if len(namespace) < 5:
            raise BrainNamespaceNameError(
                f"Namespace wrong length; 5 >= namespace >= 15; name {namespace} is {len(namespace)}"
            )
        elif len(namespace) > 15:
            raise BrainNamespaceNameError(
                f"Namespace wrong length; 5 >= namespace >= 15; name {namespace} is {len(namespace)}"
            )

        # change the namespace and acknowledge the change
        self.namespace = namespace

        # if the namespace object exists already, just add the new namespace
        if plasma.ObjectID(b"brain_namespaces_set") in self.client.list().keys():
            # add to namespaces
            namespaces = self.client.get(
                plasma.ObjectID(b"brain_namespaces_set")
            ).union([self.namespace, "default"])
            # remove old namespaces object
            self.client.delete([plasma.ObjectID(b"brain_namespaces_set")])
            # assign new namespaces object
            self.client.put(namespaces, plasma.ObjectID(b"brain_namespaces_set"))
        # otherwise, create the namespaces object and add to plasma
        else:
            self.client.put(
                set([self.namespace, "default"]),
                plasma.ObjectID(b"brain_namespaces_set"),
            )
        # return the current namespace
        return self.namespace

    def show_namespaces(self):
        return self.client.get(plasma.ObjectID(b"brain_namespaces_set"))

    def remove_namespace(self, namespace=None) -> str:
        """
        remove a namespace and all its values from Plasma

        Errors:
            BrainNamespaceRemoveDefaultError
            BrainNamespaceNotExistError
        """
        # if no namespace is defined, just remove the current namespace
        if namespace == None:
            namespace == self.namespace

        # cannot delete the default namespace
        if namespace == "default":
            raise BrainNamespaceRemoveDefaultError("Cannot remove default namespace")

        # cannot delete a namespace that doesn't exist
        if namespace not in self.show_namespaces():
            raise BrainNamespaceNotExistError(f'Namespace "{namespace}" does not exist')

        # save the current namespace
        current_namespace = self.namespace
        self.namespace = namespace

        # delete all the variables in <namespace>
        for name in self.names():
            self.forget(name)

        ## remove namespace from set of namespaces
        # get current namespaces
        namespaces = self.client.get(plasma.ObjectID(b"brain_namespaces_set")).union(
            [self.namespace, "default"]
        )
        # remove <namespace> from current namespaces set
        namespaces = namespaces - set([namespace])
        # remove the old namespaces object
        self.client.delete([plasma.ObjectID(b"brain_namespaces_set")])
        # add the new namespaces object
        self.client.put(namespaces, plasma.ObjectID(b"brain_namespaces_set"))

        # if we cleared the current namespace, change the namespace to default
        if current_namespace == namespace:
            self.namespace = "default"
        # otherwise, just change self.namespace back to what it was
        else:
            self.namespace = current_namespace

        return "Deleted namespace {}. Using namespace {}.".format(
            namespace, self.namespace
        )

    ##########################################################################################
    # UTILITY FUNCTIONS
    ##########################################################################################
    def _brain_new_ids_or_existing_ids(self, name, client):
        """
        if name exists, returns object id of that name and that client
        
        else create new ids
        """
        if self.exists(name):
            # GET THE BRAIN_OBJECT FOR THE OLD NAME
            brain_object = self._brain_names_objects(client)
            for x in brain_object:
                if x["name"] == name:
                    brain_object = x
                    break
            # NOTE THIS BEHAVIOR IS NOT EXPECTED BY THE USER
            # DELETE THE OLD NAME AND THING OBJECTS
            client.delete(
                [
                    plasma.ObjectID(brain_object["name_id"]),
                    plasma.ObjectID(brain_object["id"]),
                ]
            )
            # GET THE NEW IDS
            thing_id = plasma.ObjectID(brain_object["id"])
            name_id = plasma.ObjectID(brain_object["name_id"])
        else:
            # CREATE A NEW NAME ID AND THING ID
            name_id = self._name_to_justified_hash(name)
            thing_id = plasma.ObjectID.from_random()
        return thing_id, name_id

    def _hash(self, name: str, digest_bytes: int) -> ByteString:
        """
        return a bytestring with length hex_bytes of the name string
        """
        return hashlib.blake2b(name.encode(), digest_size=digest_bytes).digest()

    def _name_to_hash(self, name: str) -> plasma.ObjectID:
        """
        hash the name to 20 bytes
        create a plasma.ObjectId
        """
        name_hash = self._hash(name, 20)
        _id = plasma.ObjectID(name_hash)
        return _id

    def _name_to_justified_hash(self, name: str) -> plasma.ObjectID:
        """
        create an ObjectId that contains the name justified by its hash with digest length 20-len(name)
        e.g. 
        name: "this"
        16-byte hash digest: b'%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee'
        combined (20-byte): b'this%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee'
        return object id: ObjectID(7468697325149937460849fbe4c3f85698130eee)

        Errors:
            BrainNameLengthError
        """
        name_len = len(name)
        if name_len > 16:
            raise BrainNameLengthError(
                f"Name wrong length; 5 >= name >= 15; name {name} is {name_len}"
            )
        hash_len = 20 - len(name)
        encoded = name.encode()
        name_hash = self._hash(name, hash_len)
        combined = encoded + name_hash
        return plasma.ObjectID(combined)

    def _name_to_namespace_hash(self, name: str) -> plasma.ObjectID:
        """
        create an ObjectId that contains the namespace name + the hash of the name
        name: "this"
        namespace: "default"
        16-byte hash digest: b'%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee'
        combined (20-byte): b'this%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee'
        return object id: ObjectID(7468697325149937460849fbe4c3f85698130eee)
        """
        # NAMESPACE CAN'T BE SET TO AN INCORRECT SIZE
        namespace_len = len(self.namespace)
        hash_len = 20 - namespace_len
        encoded = self.namespace.encode()
        name_hash = self._hash(name, hash_len)
        combined = encoded + name_hash
        return plasma.ObjectID(combined)

    def _brain_names_ids(self):
        """get dict of names and ObjectIDs in the store"""
        names_ = self._brain_names_objects(self.client)
        return {x["name"]: plasma.ObjectID(x["id"]) for x in names_}

    def _brain_names_objects(self, client):
        """
        get a dictionary of names and ids that brain knows

        note on this: 
            - the convention is that every name that brain knows is
            stored separately as an object in the plasma store with 
            the prefix b'<namespace>'...
            - so brain gets the names by 
                - getting all object ids
                - any that have b'<namespace>' in them will be dictionaries of 
                type (numpy, general, pandas) and ObjectID, or other metadata
        """
        # get all ids
        all_ids = list(client.list().keys())

        # get the first several characters of the ObjectID representation to use to filter names
        namespace_str = str(self._brain_create_named_object(self.namespace))[9:17]

        # get all ids that contain the namespace representation
        known_ids = [x for x in all_ids if namespace_str in str(x).lower()]

        # get all actual objects (names and type) with those ids
        values = client.get(known_ids, timeout_ms=100)
        return values

    def _brain_create_named_object(self, name):
        """return a random ObjectID that has <self.namespace> in it"""
        letters = string.ascii_letters
        random_letters = "".join(
            random.choice(letters) for i in range(20 - len(self.namespace))
        )
        return plasma.ObjectID(bytes(self.namespace + random_letters, "utf-8"))

