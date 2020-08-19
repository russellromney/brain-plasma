import pytest
from pyarrow import plasma

from brain_plasma import Brain
from brain_plasma import exceptions
from brain_plasma.mock import MockPlasmaClient


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


def test_metadata_good(brain):
    brain["this"] = "that"
    assert len(brain.metadata()) == 1


def test_metadata_bad(brain):
    with pytest.raises(TypeError):
        brain.metadata("this", output="int")


def test_set_namespace_good(brain):
    assert brain.namespace == "default"
    brain.set_namespace("somespace")
    assert brain.namespace == "somespace"


def test_set_namespace_bad(brain):
    with pytest.raises(exceptions.BrainNamespaceNameError):
        brain.set_namespace("1")

    with pytest.raises(exceptions.BrainNamespaceNameError):
        brain.set_namespace("some way too long namespace")


def test_remove_namespace(brain):
    brain.set_namespace("somespace")
    brain["that"] = "this"
    brain.remove_namespace("somespace")
    assert brain.namespace == "default"
    assert not "that" in brain


def test_hash(brain):
    assert (
        brain._hash("this", 20)
        == b"\xbdVD\x9e6\xa6\x17\xc7\xb6xm:(\xf1\x8c\x84\x13\xdd-X"
    )
    assert brain._hash("this", 10) == b"}\xff, \xb7]%\x02\x0ei"


def test_name_to_hash(brain):
    assert brain._name_to_hash("this") == plasma.ObjectID(
        b"\xbdVD\x9e6\xa6\x17\xc7\xb6xm:(\xf1\x8c\x84\x13\xdd-X"
    )


def test_name_to_justified_hash(brain):
    assert (
        brain._name_to_justified_hash("this").binary()
        == b"this%\x14\x997F\x08I\xfb\xe4\xc3\xf8V\x98\x13\x0e\xee"
    )


def test_name_to_namespace_hash(brain):
    assert (
        brain._name_to_namespace_hash("this").binary()
        == b"default\xee\xd2\xee\x1a\x9do\x15ue.Y\xe1\xd1"
    )
