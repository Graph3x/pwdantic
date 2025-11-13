from pwdantic.serialization import SQLColumn
from pwdantic.datatypes import *
from copy import deepcopy
from pwdantic.exceptions import PWInvalidMigrationError


class MigrationEngine:
    def generate_migration(
        self, table: str, original: list[SQLColumn], new: list[SQLColumn]
    ) -> Migration:
        return Migration(table, [])
    
    def get_renamed_mapping(self, migration: Migration):
        mapping = {}
        for step in migration.steps:
            if type(step) == RenameCol:
                mapping[step.old_name] = step.new_name
        return mapping

    def get_migrated_cols(
        self, original: list[SQLColumn], migration: Migration
    ) -> list[SQLColumn]:

        new_cols = [deepcopy(x) for x in original]
        migration.sort()

        for step in migration.steps:
            if type(step) == AddCol:
                new_cols.append(step.column)

            elif type(step) == DropCol:
                new_cols = filter(
                    lambda x: x.name != step.column_name, new_cols
                )

            elif type(step) == RenameCol:
                for col in new_cols:
                    if col.name == step.old_name:
                        col.name = step.new_name

            elif type(step) == RetypeCol:
                for col in new_cols:
                    if col.name == step.column_name:
                        col.datatype = step.new_type

            elif type(step) == AddConstraint:
                for col in new_cols:
                    if col.name != step.column_name:
                        continue
                    if step.constraint == SQLConstraint.nullable:
                        col.nullable = True
                    if step.constraint == SQLConstraint.unique:
                        col.unique = True
                    if step.constraint == SQLConstraint.primary:
                        if (
                            len(
                                filter(
                                    lambda x: type(x) == RemoveConstraint
                                    and x.constraint == SQLConstraint.primary,
                                    migration.steps,
                                )
                            )
                            != 1
                        ):
                            raise PWInvalidMigrationError()
                        col.primary_key = True

            elif type(step) == RemoveConstraint:
                for col in new_cols:
                    if col.name != step.column_name:
                        continue
                    if step.constraint == SQLConstraint.nullable:
                        col.nullable = False
                    if step.constraint == SQLConstraint.unique:
                        col.unique = False
                    if step.constraint == SQLConstraint.primary:
                        if (
                            len(
                                filter(
                                    lambda x: type(x) == AddConstraint
                                    and x.constraint == SQLConstraint.primary,
                                    migration.steps,
                                )
                            )
                            != 1
                        ):
                            raise PWInvalidMigrationError()
                        col.primary_key = False

        return new_cols
