from dbtogo import DBModel
from dbtogo.serialization import GeneralSQLSerializer


class A(DBModel):
    a: int = None
    a2: str


class B(DBModel):
    b: list[int]
    _b: int = 4


class C(A, B):
    pass


class D1(C):
    a: int = 4
    d: str = "D"


class D2(DBModel):
    b: list[int]
    _b: int = 4
    a: int = 4
    a2: str
    d: str = "D"


def test_inheretance():
    gss = GeneralSQLSerializer()
    s1 = gss.serialize_schema("X", D1.model_json_schema(), None, ["a", "a2"])
    s2 = gss.serialize_schema("X", D2.model_json_schema(), None, ["a", "a2"])

    assert len(s1) == len(s2)
    assert s1 == s2


test_inheretance()
