import pytest
from pyarrow import plasma

from brain_plasma.brain_plasma_hash import Brain
from brain_plasma.mock import MockPlasmaClient
from brain_plasma import exceptions


@pytest.fixture(scope="function")
def brain():
    """Brain with mocked plasma_store client"""
    return Brain(ClientClass=MockPlasmaClient)


def test_init_defaults(brain):
    assert brain.path == "/tmp/plasma"
    assert brain.namespace == "default"
    assert hasattr(brain, "client")


def test_init_other():
    test = Brain(namespace="nondefault", ClientClass=MockPlasmaClient)
    assert test.namespace == "nondefault"


def test_recall_not_exist(brain):
    with pytest.raises(exceptions.BrainNameNotExistError):
        brain.recall("this")


def test_learn_exists(brain):
    brain.learn("this", "that")
    # did it work with the right value?
    assert brain.exists("this")
    assert brain.recall("this") == "that"

    # did it create metadata?
    out = brain.metadata("this")
    assert "name" in out
    assert brain.client.get(plasma.ObjectID(out["value_id"])) == "that"


def test_setitem_getitem(brain):
    brain["this"] = "that"
    assert brain["this"] == "that"


def test_forget_delitem(brain):
    brain["this"] = "that"
    del brain["this"]
    assert not brain.exists("this")


def test_exists(brain):
    brain["this"] = "that"
    assert brain.exists("this")


def test_names(brain):
    brain["this"] = "that"
    assert "this" in brain.names()

    # multiple namespaces?
    brain.set_namespace("newspace")
    brain["that"] = "other"
    assert "this" in brain.names(namespace="all")


def test_namespaces(brain):
    brain.set_namespace("newspace")
    brain["this"] = "this"
    assert brain["this"] == "this"

    brain.set_namespace("default")
    with pytest.raises(exceptions.BrainNameNotExistError):
        brain["this"]


def test_ids(brain):
    brain["this"] = "this"
    out = brain.ids()
    assert len(out)
    out = brain.metadata("this")
    print(out)


def test_object_id(brain):
    brain["this"] = "that"
    assert brain.object_id("this") == plasma.ObjectID(
        brain.metadata("this").get("value_id")
    )


def test_object_ids(brain):
    brain["this"] = "that"
    assert brain.object_ids() == {
        "this": plasma.ObjectID(brain.metadata("this").get("value_id"))
    }
