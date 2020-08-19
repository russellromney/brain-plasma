from pyarrow import plasma
import os
import random
import string
import time

# apache plasma documentation
# https://arrow.apache.org/docs/python/plasma.html


class v02Brain:
    def __init__(self, namespace="default", path="/tmp/plasma"):
        self.path = path
        self.namespace = namespace
        self.client = plasma.connect(self.path, num_retries=5)
        self.bytes = self.size()
        self.mb = "{} MB".format(round(self.bytes / 1000000))
        self.set_namespace(namespace)

    ### core functions
    def learn(self, name, thing, description=False):
        """put a given object to the plasma store"""
        # check that name is string
        if not type(name) == str:
            raise BaseException(
                'BrainError: type of name "{}" is not string'.format(name)
            )

        ### get names and brain object
        # get a "names object id"
        this_id, name_id = self._brain_new_ids_or_existing_ids(name, self.client)
        # if object is normal python object
        brain_object = {
            "name": name,
            "id": this_id.binary(),
            "description": description,
            "name_id": name_id.binary(),
            "namespace": self.namespace,
        }

        # check if name is already taken; if it is, delete its value stored at its ObjectID and delete its brain_object name index
        if self._brain_name_exists(name, self.client):
            self.forget(name)

        ### store them
        # put the thing to store
        self.client.put(thing, this_id)
        # put the name id reference to store
        self.client.put(brain_object, name_id)

        ### TODO - make specific things for pandas and numpy
        # special if object is pandas
        # special if object is numpy

    def __setitem__(self, name, item):
        self.learn(name, item)

    def recall(self, name):
        """get an object value based on its Brain name"""
        names_ = self._brain_names_ids(self.client)
        this_id = names_[name]
        self._brain_name_error(name, self.client)
        return self.client.get(this_id, timeout_ms=100)

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

    def forget(self, name):
        """delete an object based on its Brain name"""
        names_ = self._brain_names_ids(self.client)
        brain_object = self._brain_names_objects(self.client)
        for x in brain_object:
            if x["name"] == name:
                brain_object = x
                break
        this_id = names_[name]
        name_id = plasma.ObjectID(brain_object["name_id"])
        self.client.delete([this_id, name_id])

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
        """show the available bytes of the underlying plasma_store; wrapper for PlasmaClient.store_capacity()"""
        try:
            temp = self.client.put(5)
            self.client.delete([temp])
        except:
            raise BaseException
        self.bytes = self.client.store_capacity()
        self.mb = "{} MB".format(round(self.bytes / 1000000))
        return self.bytes

    def object_map(self):
        """return a dictionary of names and their associated ObjectIDs"""
        return self._brain_names_ids(self.client)

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
            raise BaseException(
                'BrainError: namespace "{}" must be at least 5 characters'.format(
                    namespace
                )
            )
        elif len(namespace) > 15:
            raise BaseException(
                'BrainError: namespace "{}" must be fewer than 15 characters'.format(
                    namespace
                )
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

    def remove_namespace(self, namespace=None):
        # if no namespace is defined, just remove the current namespace
        if namespace == None:
            namespace == self.namespace

        # cannot delete the default namespace
        if namespace == "default":
            raise BaseException("BrainError: cannot remove default namespace")

        # cannot delete a namespace that doesn't exist
        if namespace not in self.show_namespaces():
            raise BaseException(
                'BrainError: namespace "{}" does not exist'.format(namespace)
            )

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

    # utility functions
    def _brain_new_ids_or_existing_ids(self, name, client):
        """if name exists, returns object id of that name and that client; else new ids"""
        if self._brain_name_exists(name, client):
            # get the brain_object for the old name
            brain_object = self._brain_names_objects(client)
            for x in brain_object:
                if x["name"] == name:
                    brain_object = x
                    break
            # delete the old name and thing objects
            client.delete(
                [
                    plasma.ObjectID(brain_object["name_id"]),
                    plasma.ObjectID(brain_object["id"]),
                ]
            )
            # get the new ids
            thing_id = plasma.ObjectID(brain_object["id"])
            name_id = plasma.ObjectID(brain_object["name_id"])
        else:
            # create a new name id and thing id
            name_id = self._brain_create_named_object(name)
            thing_id = plasma.ObjectID.from_random()
        return thing_id, name_id

    def _brain_names_ids(self, client):
        """get dict of names and ObjectIDs in the store"""
        names_ = self._brain_names_objects(client)
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

    def _brain_name_exists(self, name, client):
        """confirm that the plasma ObjectID for a given name"""
        names_ = self._brain_names_ids(client)
        return name in names_.keys()

    def _brain_name_error(self, name, client):
        """raise error if the name does not exist"""
        if not self._brain_name_exists(name, client):
            raise BaseException(
                'BrainError: Brain does not know the name "{}" in namespace "{}"'.format(
                    name, self.namespace
                )
            )

    def _brain_create_named_object(self, name):
        """return a random ObjectID that has <self.namespace> in it"""
        letters = string.ascii_letters
        random_letters = "".join(
            random.choice(letters) for i in range(20 - len(self.namespace))
        )
        return plasma.ObjectID(bytes(self.namespace + random_letters, "utf-8"))
