# vibes

Write a rust function that takes any Type, and returns a string.
With this logic:
- if type is Option and value is None, then return "IS NULL"
- if type is Option and value is Some, then do following
- if type is String or &str, return " = " + input.replace("'","''")
- for all other types return " = " + the string representation of the value


In rust, write a macro `where!` that takes in a string as a first argument, with following arguments replacing a "{}" of the string, then outputs a string.
The following arguments are assumed to be an even amount, related to a field value set.
E.g. this statement:
`where!("select c from t WHERE 1=1 AND {}", "c1", 3, "c2", 4)`
Results in the string:
"select c from t WHERE 1=1 AND c1 = 3 AND c2 = 4"

Actually, rewrite the whole thing. This time I want this to replace all "{}" with a tuple of field value pairs, so there will always be the same number of tuple params as "{}"s.
So for example this statement:
`where!("select c from t WHERE {} AND {}", ("c1", 3), ("c2", 4))`
would output:
"select c from t WHERE c1 = 3 AND c2 = 4"


In rust, is it possible to make a function that takes a type (e.g. i64), and the function will return that type? e.g. something like `fn get_as_type(T) -> T`?


===

Please help me diagnose this rust snippet.

This does not produce a compiler error in `try_from`, any tuple passed through is accepted by the compiler:

```rust
let result = conn.query_row(&sql, [], |row| <(i64, i64, u8, i64)>::try_from(row)).unwrap();
```

However when trying to genericise, so I can pass in a tuple of any type, with this following snippet:

```rust
pub fn query_single_row_to_type<T>(dbfilepath:&Path, sql:&str, t: T) -> Result<T, Box<dyn Error>> {
    let conn = Connection::open(&dbfilepath)?;
    let result = conn.query_row(&sql, [], |row: &Row<'_>| Ok(<T>::try_from(row)))?;
    return Ok(result?);
}
```
I get this compiler error:

```
error[E0277]: the trait bound `T: From<&Row<'_>>` is not satisfied
   --> src\sql.rs:205:62
    |
205 |     let result = conn.query_row(&sql, [], |row: &Row<'_>| Ok(<T>::try_from(row)))?;
    |                                                              ^^^^^^^^^^^^^^^^^^ the trait `From<&Row<'_>>` is not implemented for `T`
    |
    = note: required for `&Row<'_>` to implement `Into<T>`
    = note: required for `T` to implement `TryFrom<&Row<'_>>`
help: consider restricting type parameter `T` with trait `From`
    |
```


With this rust function:

```rust
pub fn query_single_row_to_type<T: std::convert::From<&rusqlite::Row<'a>>>(dbfilepath:&Path, sql:&str, t: T) -> Result<T, Box<dyn Error>> {
    let conn = Connection::open(&dbfilepath)?;
    let result = conn.query_row(&sql, [], |row| Ok(<T>::try_from(row)))?;
    return Ok(result?);
}
```

I get this compiler error:

