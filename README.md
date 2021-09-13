# livedb
a software layer for the handling of historic data in relational database tables

Livedb is two things:

- a library `livedb` (a Go package) with a flexible yet rather complicated API, providing methods for the handling of all kinds of historic data, and

- a code generator `generatelivetab` which adds a simplified "API" for each kind of data the programmer describes using a JSON format.


### Motivation
Historic data is all about what data is valid when. (When does a contract start? When does the higher salary goes into effect? From when on does a new address apply?) And it's about not forgetting the past.

Livedb offers a way of dealing with all that:
- A database table stands for a kind of objects -- a type.
- Every object has an attribute `ID`.
- Every instance of a object (a record in the database table) is valid for a period; the attributes `Begin` and `Until` form the interval `[Begin; Until[`.
- For every moment in time there exists at most one instance of an object.
- Important for bookkeeping: Livedb does not allow changes of the past!


### Usage
Use `"github.com/hwheinzen/livedb"` in your project's import statements.

To get the `generatelivetab` code generator do:

`$ go install github.com/hwheinzen/livedb/cmd/generatelivetab@latest`

(But maybe you like to have all sources at hand: clone the repository then.)


### Workflow
1. Write a JSON description for each table in your project directory.
2. Run `generatelivetab` for each table (possibly via `go generate`).
3. Use generated functions in your project (e.g. `emptyXx startXx changeXx terminateXx` for the services of a data server).
4. Import `github.com/hwheinzen/livedb`.
5. Use `livedb` functions like Open/Close/Begin/Commit in your project
