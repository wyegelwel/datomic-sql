# datomic-sql

A library to be a drop in replacement for datomic.api to work with sql instead. It isn't there, but it does work for very simple datomic.api/q calls. 

Pull requests to support more of datomic.api are welcome.

## Usage

Assume we have a sql database lying around with a table `bookmarks` that looks like:

|id | type | title |
|---|------|-------|
| 1 | web  | my-cool-title |
| 2 | web  | my-cool-title2 |
| 3 | test | my-not-cool-title |

Then you could use datomic-sql to query for bookmarks of type "web". 

```clojure
(require '[datomic-sql.core :as ds])

(q '[:find ?title
         :in $ ?type
         :where 
         [_ :bookmarks/type ?type]
         [_ :bookmarks/title ?title]]
       {:classname "org.sqlite.JDBC"
        :subprotocol "sqlite"
        :subname "test.db"}
       "web")
#{[\"my-cool-title\"] [\"my-cool-title2\"]}
``` 

If your datomic queries meet the following conditions, you could (not recommended) use datomic-sql
as a replacement for datomic.api and query sql instead.
    
    * Entity selectors are specified with `_`. Essentially saying you care about a single entity or row in sql
    * Attributes in your where clause are explicitly specified (not vars)
    * Values in your where clauses are vars that are either selected (in `:find`) or bound to a value to do _equality_ selection (in `:in`)

This is to say, the library is currently a toy. 

## License

Copyright © 2017 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
