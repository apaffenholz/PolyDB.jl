"""
Main module for `PolyDB.jl` -- a package to access polyDB.
"""

module PolyDB

using Mongoc
using BSON
using JSON
using JSONSchema
using Suppressor

export PolyDBDatabase
export PolyDBCollection

export polyDB
export collections, sections, section_names
export get_collection
export find, find_one, as_dict
export collection_schema, type_of
export PolyDBDatabase

struct PolyDBDatabase
  options::Dict
  db::Mongoc.Database
end

struct PolyDBCollection
  name::String
  db::PolyDBDatabase
  coll::Mongoc.Collection
  info_coll::Mongoc.Collection
end

include("sections.jl")
include("query.jl")
include("schema.jl")
include("collections.jl")
include("polymake_types.jl")


function __init__()
  println("PolyDB.jl is a module to access PolyDB")
  println("(c) 2023-2024 Andreas Paffenholz")
end

"""
    polyDB(options::Dict=Dict())::PolyDBDatabase

  return an handle to the polyDB database
  # Examples
```jldoctest
julia> using PolyDB

julia> db = PolyDB.polyDB()
PolyDBDatabase(Dict{Any, Any}(), Database(Client(URI("mongodb://polymake:database@db.polymake.org:27017/?tls=True&tlsAllowInvalidCertificates=True")), "polydb"))

julia> typeof(db)
PolyDBDatabase
```
"""
function polyDB(user::String="polymake", password::String = "database", options::Dict=Dict())
  client = Mongoc.Client(
    "mongodb://"*user*":"*password*"@db.polymake.org:27017/?tls=True&tlsAllowInvalidCertificates=True",
  )
  Mongoc.ping(client)
  db = Nothing
  db = PolyDBDatabase(options, client["polydb"])
  return db
end

end
