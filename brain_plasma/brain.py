import traceback
from typing import ByteString, Iterable
import hashlib
from pyarrow import plasma
import os
import random
import string
import time

from .brain_client import BrainClient
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
    def __init__(
        self, namespace="default", path="/tmp/plasma", ClientClass=BrainClient
    ):
        self.path = path
        self.namespace = namespace
        self.client = ClientClass(path)
        self.bytes = self.size()
        self.mb = "{} MB".format(round(self.bytes / 1000000))
        self.set_namespace(namespace)

    ##########################################################################################
    # CORE FUNCTIONS
    ##########################################################################################
    def __setitem__(self, name, item):
        self.learn(name, item)

    def __getitem__(self, name):
        return self.recall(name)

    def __delitem__(self, name):
        return self.forget(name)

    def __contains__(self, name):
        return name in self.names()

    def __len__(self):
        return len(self.names())

    @property
    def reserved_names(self):
        return ["brain_namespaces_set"]

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
        """
        return a list of the names that brain knows
        in all namespaces or only in current (default)

        if namespace = "all", returns names from all namespaces
        """
        current_namespace = self.namespace
        if namespace is None:
            namespace = self.namespace

        names = []
        if namespace == "all":
            # FOR EACH NAMESPACE, ADD THE NAME OBJECTS TO THE LIST OF NAMES
            for namespace in self.namespaces():
                self.namespace = namespace
                names.extend([x["name"] for x in self.metadata(output="list")])
        else:
            # RETURN ALL THE NAMES AND OBJECT_IDS IN THAT NAMESPACE ONLY
            names = [
                x["name"]
                for x in self.metadata(output="list")
                if x["namespace"] == self.namespace
            ]

        self.namespace = current_namespace
        return names

    def ids(self):
        """return list of Object IDs the brain knows that are attached to names"""
        names_ = self.metadata()
        return [plasma.ObjectID(x["value_id"]) for x in names_.values()]

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
            # IF THIS DOESN'T WORK, CLIENT IS DISCONNECTED
            temp = plasma.ObjectID.from_random()
            self.client.put(5, temp)
            self.client.delete([temp])
        except:
            raise BrainClientDisconnectedError
        self.bytes = self.client.store_capacity()
        self.mb = "{} MB".format(round(self.bytes / 1000000))
        return self.bytes

    def object_id(self, name: str) -> plasma.ObjectID:
        """
        get the ObjectId of the value in the store for name

        returns None if it doesn't exist
        """
        if not self.exists(name):
            return None
        metadata = self.metadata(name)
        return plasma.ObjectID(metadata["value_id"])

    def object_ids(self) -> dict:
        """
        return a dictionary of names and their ObjectIDs
        
        limited to names in the current namespace
        """
        names_ = self.metadata().values()
        return {x["name"]: plasma.ObjectID(x["value_id"]) for x in names_}

    def metadata(self, *names, output: str = "dict") -> Iterable:
        """
        return a dict/list of all names and their associated metadata in current namespace
        
        accepts one or many names
        if only one name, only grabs one metadata
        otherwise, grabs all the metadata and returns them in a dictionary/list

        note on this: 
            every name metadata is stored as a metadata object with the prefix b'<namespace>'
            so brain gets the names by 
                getting all object ids
                any that have b'<namespace>' in them will be dictionaries of metadata

        Errors:
            TypeError
        """
        if output not in ["dict", "list"]:
            raise TypeError('Output must be "list" or "dict"')

        if len(names) == 1:
            name = names[0]
            if not self.exists(name):
                return None
            metadata_id = self._name_to_namespace_hash(name)
            metadata = self.client.get(metadata_id)
            return metadata

        # GET ALL IDS IN THE STORE
        all_ids = list(self.client.list().keys())

        # GET THE FIRST SEVERAL CHARACTERS OF THE OBJECTID REPRESENTATION TO USE TO FILTER NAMES
        namespace_str = self.namespace.encode()

        # GET ALL IDS THAT CONTAIN THE NAMESPACE REPRESENTATION
        # I.E. ALL THE METADATA
        known_ids = [x for x in all_ids if x.binary().startswith(namespace_str)]

        # GET ALL ACTUAL OBJECTS (NAMES AND TYPE) WITH THOSE IDS
        all_metadata = self.client.get(known_ids, timeout_ms=100)

        if output == "dict":
            all_metadata = {meta["name"]: meta for meta in all_metadata}

        # RETURNS ALL NAMES IF NO NAMES ARE SPECIFIED
        if len(names) == 0:
            return all_metadata

        # RETURN ONLY THE NAMES SPECIFIED; NONE IF DOESN'T EXIST
        else:
            if output == "dict":
                return {name: all_metadata.get(name) for name in names}
            return all_metadata

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
        """
        either return the current namespace or change the current namespace to something new
        """
        if namespace is None:
            return self.namespace

        # MUST BE AT LEAST FIVE CHARACTERS AND FEWER THAN 15
        if len(namespace) < 5:
            raise BrainNamespaceNameError(
                f"Namespace wrong length; 5 >= namespace >= 15; name {namespace} is {len(namespace)}"
            )
        elif len(namespace) > 15:
            raise BrainNamespaceNameError(
                f"Namespace wrong length; 5 >= namespace >= 15; name {namespace} is {len(namespace)}"
            )

        # CHANGE THE NAMESPACE AND ACKNOWLEDGE THE CHANGE
        self.namespace = namespace

        # IF THE NAMESPACE OBJECT EXISTS ALREADY, JUST ADD THE NEW NAMESPACE
        if plasma.ObjectID(b"brain_namespaces_set") in self.client.list().keys():
            # ADD TO NAMESPACES
            namespaces = self.client.get(
                plasma.ObjectID(b"brain_namespaces_set")
            ).union([self.namespace, "default"])
            # REMOVE OLD NAMESPACES OBJECT
            self.client.delete([plasma.ObjectID(b"brain_namespaces_set")])
            # ASSIGN NEW NAMESPACES OBJECT
            self.client.put(namespaces, plasma.ObjectID(b"brain_namespaces_set"))

        # OTHERWISE, CREATE THE NAMESPACES OBJECT AND ADD TO PLASMA
        else:
            self.client.put(
                set([self.namespace, "default"]),
                plasma.ObjectID(b"brain_namespaces_set"),
            )

        # RETURN THE CURRENT NAMESPACE
        return self.namespace

    def namespaces(self):
        """
        return set of all namespaces available in the store
        """
        return self.client.get(plasma.ObjectID(b"brain_namespaces_set"))

    def remove_namespace(self, namespace=None) -> str:
        """
        remove a namespace and all its values from Plasma

        Errors:
            BrainNamespaceRemoveDefaultError
            BrainNamespaceNotExistError
        """
        # IF NO NAMESPACE IS DEFINED, JUST REMOVE THE CURRENT NAMESPACE
        if namespace == None:
            namespace == self.namespace

        # CANNOT DELETE THE DEFAULT NAMESPACE
        if namespace == "default":
            raise BrainNamespaceRemoveDefaultError("Cannot remove default namespace")

        # CANNOT DELETE A NAMESPACE THAT DOESN'T EXIST
        if namespace not in self.namespaces():
            raise BrainNamespaceNotExistError(f'Namespace "{namespace}" does not exist')

        # SAVE THE CURRENT NAMESPACE
        current_namespace = self.namespace
        self.namespace = namespace

        # DELETE ALL THE VARIABLES IN <NAMESPACE>
        for name in self.names():
            self.forget(name)

        ## REMOVE NAMESPACE FROM SET OF NAMESPACES
        # GET CURRENT NAMESPACES
        namespaces = self.client.get(plasma.ObjectID(b"brain_namespaces_set")).union(
            [self.namespace, "default"]
        )
        # REMOVE <NAMESPACE> FROM CURRENT NAMESPACES SET
        namespaces = namespaces - set([namespace])
        # REMOVE THE OLD NAMESPACES OBJECT
        self.client.delete([plasma.ObjectID(b"brain_namespaces_set")])
        # ADD THE NEW NAMESPACES OBJECT
        self.client.put(namespaces, plasma.ObjectID(b"brain_namespaces_set"))

        # IF WE CLEARED THE CURRENT NAMESPACE, CHANGE THE NAMESPACE TO DEFAULT
        if current_namespace == namespace:
            self.namespace = "default"
        # OTHERWISE, JUST CHANGE SELF.NAMESPACE BACK TO WHAT IT WAS
        else:
            self.namespace = current_namespace

        return "Deleted namespace {}. Using namespace {}.".format(
            namespace, self.namespace
        )

    ##########################################################################################
    # UTILITY FUNCTIONS
    ##########################################################################################
    def _hash(self, name: str, digest_bytes: int) -> ByteString:
        """
        input a name str
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

    def _name_to_namespace_hash(
        self, name: str, namespace: str = None
    ) -> plasma.ObjectID:
        """
        create an ObjectId that contains the namespace name + the hash of the name
        name: "this"
        namespace: "default"
        16-byte hash digest: b'%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee'
        combined (20-byte): b'this%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee'
        return object id: ObjectID(7468697325149937460849fbe4c3f85698130eee)
        """
        if not namespace:
            namespace = self.namespace

        # NAMESPACE CAN'T BE SET TO AN INCORRECT SIZE
        namespace_len = len(namespace)
        hash_len = 20 - namespace_len
        encoded = namespace.encode()
        name_hash = self._hash(name, hash_len)
        combined = encoded + name_hash
        return plasma.ObjectID(combined)
