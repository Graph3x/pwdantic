![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/Graph3x/dbtogo/ci.yml)
![PyPI - Downloads](https://img.shields.io/pypi/dm/dbtogo)


# DBTogo
Python ORM with focus on simplicity and compatibility.
DBTogo is made to allow you to interact with SQL databases in Python as simply and seamlessly as possible. Its goal is to completely abstract the db away, providing a simple Python native interface.

Advantages of DBTogo include:
- full Pydantic integration
- leverages native Python annotation
- focuses heavily on simplicity and reducing boilerplate

### CURRENT STATE

DBTogo currently supports:
- Full crud functionality
- Basic migrations
- Sqlite integration
- Native Pydantic model support with no limitations

However, the project is currently not mature enough to be used in larger production project.

### INSTALLATION

DBTogo requires Python 3.12 or newer and Pydantic.
It can be installed with `pip install dbtogo`

### EXAMPLE
```
from dbtogo import DBEngineFactory, DBModel


class Duck(DBModel):
    duck_id: int | None = None
    name: str
    color: str = "Brown"
    age: int | None = None
    shopping_list: list[str] = ["bread crumbs"]
    children: list["Duck"] = []

    def quack(self):
        print(f"Hi! I am {self.name} age {self.age} quack!")
```
DBTogos base class DBModel is just a regular python class (with attributes defined on class level for Pydantic support). It can even have objects as attributes! 

```
    engine = DBEngineFactory.create_sqlite3_engine("test.db")

    Duck.bind(engine, primary_key="duck_id", unique=["name"])
```

To use the DBModel class, you need to bind it with a DBEngine. This also allows you to set unique fields and specify the primary key. Nullable fields are set by including None in the type annotation of the nullable field.

```
    mc_duck_junior = Duck(
        name="Junior",
        age=15,
        shopping_list=["rohlik", "gothaj"],
    )
    mc_duck_junior.save()

    mc_duck = Duck(name="McDuck", color="Yellow", age=45, children=[mc_duck_junior])
    mc_duck.save()

```
Calling the .save() method saves the object to the db. If the object is already stored in the db, it instead updates it. You can modify any attribute of the object including the primary key!

```
    mc_duck = Duck.get(name="McDuck")
    mc_duck.children[0].quack()
```
.get() method returns the object from the db (or identity cache). You can still access any of attributes/methods of the class (even the non primitive ones)!

### License
This project uses the MIT license.