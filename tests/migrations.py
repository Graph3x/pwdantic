from pwdantic.pwdantic import PWModel, PWEngineFactory, PWEngine
from pwdantic.datatypes import *


class MigrationTestModelOld(PWModel):
    pk: int | None = None
    unq_string: str = "asdf"
    nullable_int: int | None = None
    modify_me: int

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="pk",
            unique=["unq_string", "modify_me"],
            table="migration_test",
        )


class MigrationTestModelNew(PWModel):
    pk: int | None = None
    unq_string: str
    the_same_int: int | None = None
    new_col: str | None = "default"

    @classmethod
    def bind(cls, engine):
        super().bind(
            engine,
            primary_key="unq_string",
            unique=["pk"],
            table="migration_test",
        )


def automatic_migration(engine: PWEngine):
    MigrationTestModelOld.bind(engine)
    MigrationTestModelNew.bind(engine)


def manual_migration(engine: PWEngine):
    MigrationTestModelOld.bind(engine)

    a = MigrationTestModelOld(unq_string="hello", modify_me=8)
    b = MigrationTestModelOld(modify_me=5, nullable_int=1)

    a.save()
    b.save()

    valid_migration_steps = [
        RenameCol("nullable_int", "the_same_int"),
        RenameCol("modify_me", "i_am_modified"),
        AddConstraint("modify_me", SQLConstraint.nullable),
        RemoveConstraint("modify_me", SQLConstraint.unique),
        AddCol(SQLColumn("new_col", SQLType.string.value, True, "default")),
    ]

    valid_migration = Migration("migration_test", valid_migration_steps)

    engine.execute_migration(valid_migration, False)

    a.delete()
    b.delete()


def main():
    engine = PWEngineFactory.create_sqlite3_engine("test.db")
    manual_migration(engine)


if __name__ == "__main__":
    main()
