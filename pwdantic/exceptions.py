class PWException(Exception):
    pass


NO_BIND = PWException("You need to bind this model to a db before first use")

INVALID_TYPES = PWException("Invalid types in your schema")
