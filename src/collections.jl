"""
    collections(db::PolyDBDatabase; filter::Union{Nothing,Regex} = nothing, further_options::Union{Nothing,Mongoc.BSON}=nothing)

obtain a list of collections in polyDB, possibly filtered by the regular expression in user_filter
# Examples
```julia-repl
julia> using PolyDB

julia> db = PolyDB.polyDB()
PolyDBDatabase(Dict{Any, Any}(), Database(Client(URI("mongodb://polymake:database@db.polymake.org:27017/?tls=True&tlsAllowInvalidCertificates=True")), "polydb"))

julia> PolyDB.collections(db, filter=r"Polytopes.Combinatorial")
4-element Vector{String}:
"Polytopes.Combinatorial.FacesBirkhoffPolytope"
"Polytopes.Combinatorial.SmallSpheresDim4"
"Polytopes.Combinatorial.01Polytopes"
"Polytopes.Combinatorial.CombinatorialTypes"
```
"""
function collections(
  db::PolyDBDatabase;
  filter::Union{Nothing,Regex}=nothing,
  further_options::Union{Nothing,Mongoc.BSON}=nothing,
)
  if isnothing(further_options)
    further_options = Mongoc.BSON()
  end
  further_options["authorizedCollections"] = true
  further_options["nameOnly"] = true

  query_filter = "^_collectionInfo\\."
  further_options["filter"] = Dict("name" => Dict("\$regex" => query_filter))
  all_collections = Mongoc.get_collection_names(db.db; options=further_options)
  all_collections = map((x) -> chop(x; head=16, tail=0), all_collections)

  collections = Vector{String}()
  if !isnothing(filter)
    for e in all_collections
      if !isnothing(match(filter, e, 1))
        push!(collections, e)
      end
    end
  else
    collections=all_collections
  end

  return collections
end

"""
    get_collection(db::PolyDBDatabase, name::String)::PolyDBCollection

returns a handle for a collection in polyDB
# Examples
```julia-repl
julia> db = PolyDB.polyDB();

julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.Reflexive");

julia> typeof(coll)
PolyDB.PolyDBCollection
```
"""
function get_collection(db::PolyDBDatabase, name::String)
  return PolyDBCollection(name, db, db.db[name], db.db["_collectionInfo." * name])
end
