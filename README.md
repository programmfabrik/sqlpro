# go-sqlpro

## WIP 

This package is under active development and work-in-progress.


## Query(target interface{}, query string, args...)





If you want to insert a custom type (e.g. a struct) into the db your type needs to fullfill follwing interface functions
for (un-)marshalling from/to the database.

**Marshalling to DB**
```
func (f Metadata) Value() (driver.Value, error)
```

**Unmarshalling from DB**
```
func (f *Metadata) Scan(v interface{}) error`
```

**json** is supported as tag!