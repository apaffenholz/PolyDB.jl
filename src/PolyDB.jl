"""
Main module for `PolyDB.jl` -- a package to access polyDB.
"""

module PolyDB

import NetworkOptions

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
export find, find_one, aggregate, distinct, as_dict, sample
export collection_schema, type_of
export PolyDBDatabase

struct PolyDBDatabase
  db::Mongoc.Database
  options::Union{Dict,Nothing}
  uri::String
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
function polyDB(;
    user::String="polymake",
    password::String = "database",
    host::String = "db.polymake.org",
    port::Int = 27017,
    ssl::Bool = true,
    tlsAllowInvalidHostnames::Bool = false,
    tlsAllowInvalidCertificates::Bool = false
  )

  uri = get(ENV, "POLYDB_URI", nothing)

  if isnothing(uri)
    options = Dict(
      user => user,
      password => password,
      host => host,
      port => port,
      ssl => ssl,
      tlsAllowInvalidCertificates => tlsAllowInvalidCertificates,
      tlsAllowInvalidHostnames  => tlsAllowInvalidHostnames
    )

    uri = "mongodb://" * user * ":" * password * "@" * host * ":" * string(port) * "/?authSource=admin"
    if ssl
       uri *= "&ssl=true&sslCertificateAuthorityFile="*NetworkOptions.ca_roots_path()
    end
    if tlsAllowInvalidCertificates
       uri *= "&tlsAllowInvalidCertificates=true"
    end
    if tlsAllowInvalidHostnames
       uri *= "&tlsAllowInvalidHostnames=true"
    end
  else
    options = nothing
  end

  client = Mongoc.Client(uri)
  try
    Mongoc.ping(client)
  catch e
    println("Connection to database failed with error: " * string(e))
  end

  db = PolyDBDatabase(client["polydb"],options,uri)
  return db
end

end
