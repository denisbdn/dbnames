# dbnames
go library help to build query

The library helps to build queries to the database using annotations in the description of the structure fields.

Usage:
```go
// add annotation db:"field_name" to structure fields
type DBData struct {
	Crc      uint32        `db:"crc"`
	Create   MYSQLDATETIME `db:"create"`
	Desc     string        `db:"desc"`
	NotFound string
}

// build query fields
allFields := BuildFields("some_table", DBData{})

// use it
query := fmt.Sprintf("SELECT %s FROM `%s` LIMIT 1;", strings.Join(allFields, ", "), "some_table")

fmt.println(query)
```
Output:
```go
SELECT `some_table`.`crc` `some_table`.`create` `some_table`.`desc` FROM `some_table` LIMIT 1;
```
