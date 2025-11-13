class PWNoBindError(Exception):
    def __init__(self):
        super().__init__(
            "You need to bind this model to a db before first use"
        )


class PWInvalidTypeError(Exception):
    def __init__(self):
        super().__init__("Invalid types in your schema")


class PWBindViolationError(Exception):
    def __init__(self):
        super().__init__(
            "You cannot change the primary key of a bound object"
        )


class PWUnboundDeleteError(Exception):
    def __init__(self):
        super().__init__("You cannot delete an object that is not in the db")


class PWMigrationError(Exception):
    def __init__(self):
        super().__init__(
            "PW cant automatically migrate the schemas, consider manual migration"
        )


class PWDestructiveMigrationError(Exception):
    def __init__(self):
        super().__init__(
            "This migration includes destructive actions, but wasnt executed with force=True"
        )


class PWInvalidMigrationError(Exception):
    def __init__(self):
        super().__init__("This migration is not valid")
