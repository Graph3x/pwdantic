from pydantic import BaseModel
import abc
import sqlite3
from typing import Any, Self

from pwdantic.exceptions import *
from pwdantic.sqlite import SqliteEngine
from pwdantic.datatypes import PWEngine, SQLColumn

from pwdantic.serialization import GeneralSQLSerializer

DEFAULT_PRIM_KEYS = ["id", "primary_key", "uuid"]


class PWEngineFactory(abc.ABC):
    @staticmethod
    def create_sqlite3_engine(database: str = "") -> PWEngine:
        conn = sqlite3.connect(database)
        return SqliteEngine(conn)


def bound(func):
    def wrapper(cls, *args, **kwargs):
        if "db" not in dir(cls):
            raise PWNoBindError()

        return func(cls, *args, **kwargs)

    return wrapper


class PWModel(BaseModel):
    @classmethod
    def bind(
        cls,
        db: PWEngine,
        primary_key: str | None = None,
        unique: list[str] = [],
        table: str = None
    ):
        cls.db = db
        table = table if table is not None else cls.__name__


        columns = GeneralSQLSerializer().serialize_schema(
            table, cls.model_json_schema(), primary_key, unique
        )

        if primary_key is None:
            for prim in DEFAULT_PRIM_KEYS:
                if prim in [x.name for x in columns]:
                    continue
                primary_key = prim
                columns.append(SQLColumn(prim, int, False, None, True, True))

        cls._primary = primary_key

        cls.table = table

        db.migrate(table, columns)

    @classmethod
    @bound
    def get(cls, **kwargs) -> Self:
        data = cls.db.select("*", cls.table, kwargs)
        if len(data) < 1:
            return None
        object = GeneralSQLSerializer().deserialize_object(cls, data[0])
        setattr(
            object, "_data_bind", getattr(object, object.__class__._primary)
        )
        return object

    def _create(self):
        obj_data = GeneralSQLSerializer().serialize_object(self)

        insert_bind = self.db.insert(self.__class__.table, obj_data)
        bind_attr = getattr(self, self.__class__._primary)
        data_bind = bind_attr if bind_attr != None else insert_bind
        setattr(self, "_data_bind", data_bind)

    def _update(self):
        bind = self._data_bind
        if getattr(self, self.__class__._primary) != bind:
            raise PWBindViolationError()

        obj_data = GeneralSQLSerializer().serialize_object(self)
        self.db.update(self.__class__.table, obj_data, self.__class__._primary)

    @bound
    def save(self):
        if getattr(self, "_data_bind", None) is None:
            return self._create()
        return self._update()

    @bound
    def delete(self):
        if getattr(self, "_data_bind", None) is None:
            raise PWUnboundDeleteError()

        primary_key = self.__class__._primary
        primary_value = self._data_bind

        self.db.delete(self.__class__.table, primary_key, primary_value)
        self._data_bind = None

    @classmethod
    @bound
    def all(cls) -> list[Self]:
        data = cls.db.select("*", cls.table)

        objects = []
        for row in data:
            object = GeneralSQLSerializer().deserialize_object(cls, row)
            setattr(
                object,
                "_data_bind",
                getattr(object, object.__class__._primary),
            )
            objects.append(object)

        return objects
