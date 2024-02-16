module PolyDB

using Mongoc
using BSON
using JSON
using JSONSchema

export PolyDBDatabase
export PolyDBCollection

export polyDB
export collections, sections, section_names
export get_collection
export find, find_one
export schema, type_of
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

"""
    polyDB(options::Dict=Dict())::PolyDBDatabase

return an handle to the polyDB database
# Examples
```julia-repl
julia> db = PolyDB.polyDB()

julia> typeof(db)
PolyDB.PolyDBDatabase
```
"""
function polyDB(user::String="polymake", password::String = "database", options::Dict=Dict())
  client = Mongoc.Client(
    "mongodb://"*user*":"*password*"@db.polymake.org:27017/?tls=True&tlsAllowInvalidCertificates=True",
  )
  Mongoc.ping(client)
  return PolyDBDatabase(options, client["polydb"])
end

"""
    collections(db::PolyDBDatabase; filter::Union{Nothing,Regex} = nothing, further_options::Union{Nothing,Mongoc.BSON}=nothing)

obtain a list of collections in polyDB, possibly filtered by the regular expression in user_filter
# Examples
```julia-repl
julia> db = PolyDB.polyDB()

julia> PolyDB.collections(db, r"Polytopes.Combinatorial")
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
  if further_options == nothing
    further_options = Mongoc.BSON()
  end
  further_options["authorizedCollections"] = true
  further_options["nameOnly"] = true

  query_filter = "^_collectionInfo\\."
  further_options["filter"] = Dict("name" => Dict("\$regex" => query_filter))
  all_collections = Mongoc.get_collection_names(db.db; options=further_options)
  all_collections = map((x) -> chop(x; head=16, tail=0), all_collections)

  collections = Vector{String}()
  if filter != nothing
    for e in all_collections
      if match(filter, e, 1) != nothing
        push!(collections, e)
      end
    end
  end

  return collections
end

"""
    section_names(db::PolyDBDatabase; filter::Union{Nothing,Regex} = nothing, further_options::Union{Nothing,Mongoc.BSON}=nothing)

obtain a list of all section names matching the filter
```julia-repl
julia> db = PolyDB.polyDB()

julia> PolyDB.section_names(db, filter = r"Latt")
1-element Vector{String}:
 "Polytopes.Lattice"
```
"""
function section_names(
  db::PolyDBDatabase;
  filter::Union{Nothing,Regex}=nothing,
  further_options::Union{Nothing,Mongoc.BSON}=nothing,
)::Vector{String}
  if further_options == nothing
    further_options = Mongoc.BSON()
  end
  further_options["authorizedCollections"] = true
  further_options["nameOnly"] = true

  query_filter = "^_sectionInfo\\."
  further_options["filter"] = Dict("name" => Dict("\$regex" => query_filter))
  all_sections = Mongoc.get_collection_names(db.db; options=further_options)
  all_sections = map((x) -> chop(x; head=13, tail=0), all_sections)

  sections = Vector{String}()
  if filter != nothing
    for e in all_sections
      if match(filter, e, 1) != nothing
        push!(sections, e)
      end
    end
  end

  return sections
end

"""
    sections(db::PolyDBDatabase; filter::Union{Nothing,Regex} = nothing, with_collections::Bool = false, recursive::Bool = false, further_options::Union{Nothing,Mongoc.BSON}=nothing)

returns a list of all sections mathcing the filter. If recursive is true, then recursively also all subsections are returned. If with_collections is true, then also all collections are included in the result
FIXME: This function is not fully implemented yet, it only works up to two nested sections (however, currently polyDB has no deeper nested sections, so this is currently sufficient).
# Examples
```julia-repl
julia> db = PolyDB.polyDB()

julia> print(json(PolyDB.sections(db, filter = r"Latt", recursive=false, with_collections = true),4))
[
    {
        "name": "Polytopes.Lattice",
        "collections": [
            "SmoothReflexive",
            "ExceptionalMaximalHollow",
            "01Polytopes",
            "SmallVolume",
            "FewLatticePoints3D",
            "Panoptigons",
            "NonSpanning3D",
            "Reflexive"
        ]
    }
]


julia> print(json(PolyDB.sections(db, filter = r"Pol", recursive=true, with_collections = true),4))
[
    {
        "name": "Polytopes",
        "collections": [],
        "subsections": [
            {
                "name": "Lattice",
                "collections": [
                    "SmoothReflexive",
                    "ExceptionalMaximalHollow",
                    "01Polytopes",
                    "SmallVolume",
                    "FewLatticePoints3D",
                    "Panoptigons",
                    "NonSpanning3D",
                    "Reflexive"
                ],
                "subsections": []
            },
            {
                "name": "Combinatorial",
                "collections": [
                    "FacesBirkhoffPolytope",
                    "SmallSpheresDim4",
                    "01Polytopes",
                    "CombinatorialTypes"
                ],
                "subsections": []
            },
            {
                "name": "Geometric",
                "collections": [
                    "01Polytopes"
                ],
                "subsections": []
            }
        ]
    }
]
```
"""
function sections(
  db::PolyDBDatabase;
  filter::Union{Nothing,Regex}=nothing,
  with_collections::Bool=false,
  recursive::Bool=false,
  further_options::Union{Nothing,Mongoc.BSON}=nothing,
)
  if further_options == nothing
    further_options = Mongoc.BSON()
  end
  further_options["authorizedCollections"] = true
  further_options["nameOnly"] = true

  query_filter = "^_sectionInfo\\."
  further_options["filter"] = Dict("name" => Dict("\$regex" => query_filter))
  all_sections = Mongoc.get_collection_names(db.db; options=further_options)
  all_sections = map((x) -> chop(x; head=13, tail=0), all_sections)

  if recursive
    if filter == nothing
      filter = r"^[a-zA-Z0-9_-]+$"
    else
      filter *= r"[a-zA-Z0-9_-]*$"
    end
  end

  sections = Vector{String}()
  if filter != nothing
    for e in all_sections
      if match(filter, e, 1) != nothing
        push!(sections, e)
      end
    end
  end

  for e in sections
    filter!(f -> (match(Regex(e * "."), f, 1) == nothing), sections)
  end

  sections_array = Dict{String,Any}[]

  for e in sections
    subsections = Dict{String,Any}()
    subsections["name"] = e
    if with_collections
      subsections["collections"] = map(
        (x) -> chop(x; head=length(e) + 1, tail=0),
        collections(db; filter=e * r"\.[A-Za-z0-9_-]+$"),
      )
    end
    if recursive
      subsections["subsections"] = Dict{String,Any}[]
      for f in all_sections
        if match(Regex(e * "."), f, 1) != nothing
          subsection_string = chop(f; head=length(e) + 1, tail=0)
          subsections_dict = Dict(
            "name" => subsection_string, "subsections" => Dict{String,Any}[]
          )
          if with_collections
            subsections_dict["collections"] = map(
              (x) -> chop(x; head=length(f) + 1, tail=0), collections(db; filter=Regex(f))
            )
          end
          push!(subsections["subsections"], subsections_dict)
        end
      end
    end
    push!(sections_array, subsections)
  end

  return sections_array
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

"""
    find_one(collection::PolyDBCollection; query::Union{Dict,Nothing}=nothing, projection::Union{Dict,Nothing}=nothing, skip::Union{Int,Nothing}=nothing, further_options::Union{Nothing,Mongoc.BSON}=nothing)

Retrive one element from the collection that matches the given query
# Examples
```julia-repl
julia> db = PolyDB.polyDB();
julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.Reflexive");
julia> PolyDB.find_one(coll, query = Dict("N_VERTICES" => 7), skip = 177445)
Mongoc.BSON with 41 entries:
  "_id"                       => "v07-000099999"
  "FACETS"                    => Any[Any["1", "1", "-1", "0", "-1"], Any["1", "-1", "2", "0", "0"], Any["1", "-1", "-1", "0", "6"], An…
  "ALTSHULER_DET"             => 4
  "VERTEX_SIZES"              => Any[6, 4, 5, 4, 5, 4, 4]
  "FACET_SIZES"               => Any[6, 5, 5, 4, 4, 4, 4]
  "SIMPLICIAL"                => false
  "H_STAR_VECTOR"             => Any[1, 33, 100, 33, 1]
  "H_11"                      => 32
  "CONE_DIM"                  => 5
  "TERMINAL"                  => false
  "VERY_AMPLE"                => true
  "SMOOTH"                    => false
  "EHRHART_POLYNOMIAL"        => Any[Dict{String, Any}("4"=>"7", "1"=>"9/2", "0"=>"1", "2"=>"23/2", "3"=>"14")]
  "CENTROID"                  => Any["1", "-11/7", "-6/7", "-5/7", "-4/35"]
  "N_EDGES"                   => 16
  "_type"                     => "polytope::Polytope<Rational>"
  "POLAR_SMOOTH"              => false
  "SELF_DUAL"                 => true
  "N_HILBERT_BASIS"           => 38
  "N_RIDGES"                  => 16
  "N_VERTICES"                => 7
  "LATTICE_DEGREE"            => 4
  "DIAMETER"                  => 2
  "BALANCED"                  => false
  "VERTICES"                  => Any[Any["1", "1", "0", "0", "0"], Any["1", "0", "1", "0", "0"], Any["1", "0", "1", "3", "0"], Any["1"…
  "N_LATTICE_POINTS"          => 38
  "N_BOUNDARY_LATTICE_POINTS" => 37
  "SIMPLE"                    => false
  "_info"                     => Dict{String, Any}("credits"=>Dict{String, Any}("ppl"=>"  The Parma Polyhedra Library ([[wiki:external…
  "_attrs"                    => Dict{String, Any}("DIAMETER"=>Dict{String, Any}("method"=>true), "DIM"=>Dict{String, Any}("method"=>t…
  "H_12"                      => 26
  "_polyDB"                   => Dict{String, Any}("uri"=>"https://polymake.org", "section"=>"Polytopes.Lattice", "creation_date"=>"20…
  "REFLEXIVE"                 => true
  "EULER_CHARACTERISTIC"      => 12
  "LATTICE_VOLUME"            => 168
  "FACET_WIDTHS"              => Any[2, 3, 12, 4, 6, 6, 12]
  "DIM"                       => 4
  "_ns"                       => Dict{String, Any}("polymake"=>Any["https://polymake.org", "3.4"])
  "N_FACETS"                  => 7
  "F_VECTOR"                  => Any[7, 16, 16, 7]
  "NORMAL"                    => true
```
"""
function find_one(
  collection::PolyDBCollection;
  query::Union{Dict,Nothing}=nothing,
  projection::Union{Dict,Nothing}=nothing,
  skip::Union{Int,Nothing}=nothing,
  further_options::Union{Nothing,Mongoc.BSON}=nothing,
)
  if query == nothing
    query = Dict()
  end
  if further_options == nothing
    further_options = Mongoc.BSON()
  end
  if skip != nothing
    further_options["skip"] = skip
  end
  if projection != nothing
    further_options["projection"] = projection
  end
  return Mongoc.find_one(collection.coll, Mongoc.BSON(query); options=further_options)
end

"""
    find(collection::PolyDBCollection; query::Union{Dict,Nothing}=nothing, projection::Union{Dict,Nothing}=nothing, skip::Union{Int,Nothing}=nothing, limit::Union{Int,Nothing}=nothing, further_options::Union{Nothing,Mongoc.BSON}=nothing)

Obtain a cursor over all results in a collection matching the given query
# Examples
```julia-repl
julia> db = PolyDB.polyDB();
julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.Reflexive");
julia> cursor = PolyDB.find(coll, query = Dict("N_VERTICES" => 7), skip = 123456, limit = 5)
julia> for e in cursor
  @printf "%d " e["N_FACETS"]
end
7 11 9 8 9
```
"""
function find(
  collection::PolyDBCollection;
  query::Union{Dict,Nothing}=nothing,
  projection::Union{Dict,Nothing}=nothing,
  skip::Union{Int,Nothing}=nothing,
  limit::Union{Int,Nothing}=nothing,
  further_options::Union{Nothing,Mongoc.BSON}=nothing,
)::Mongoc.Cursor
  if query == nothing
    query = Dict()
  end
  if further_options == nothing
    further_options = Mongoc.BSON()
  end
  if skip != nothing
    further_options["skip"] = skip
  end
  if limit != nothing
    further_options["limit"] = limit
  end
  if projection != nothing
    further_options["projection"] = projection
  end
  return Mongoc.find(collection.coll, Mongoc.BSON(query); options=further_options)
end

# replaces all __ in the schema with $ in keys
# operates by converting to string, using string replacement, and converting back to JSON
# FIXME: check if there is a faster method that directly modifies the keys
# "__" is in the schema coming from MongDB as $ is a special char
function replace_underscores(schema::Dict)::Mongoc.BSON
  schema_string = JSON.json(schema)
  schema_string = replace(schema_string, "__" => raw"$")
  return JSON.parse(schema_string)
end

"""
    schema(collection::PolyDBCollection)

obtain the JSON schema for a collection
"""
function schema(collection::PolyDBCollection)::Dict
  res = Mongoc.find_one(collection.info_coll, Mongoc.BSON("""{ "_id" : "schema.2.1" }"""))
  schema = replace_underscores(res["schema"])
  return schema
end

polymake_templated_types_one_argument = [
  "Array", "Vector", "Serialized", "IncidenceMatrix", "Set", "Graph", "SparseVector"
]
polymake_templated_types_two_arguments = [
  "Matrix", "Pair", "UniPolynomial", "HashMap", "Map", "Polynomial", "SparseMatrix"
]

build_polymake_type(type::Vector{SubString{String}}) =
  build_polymake_type(Vector{String}(type))

function build_polymake_type(type::Vector{String})
  item = popfirst!(type)
  typedef = item
  if item in polymake_templated_types_one_argument
    typedef *= "<"
    typedef *= build_polymake_type(type)
    typedef *= ">"
  elseif item in polymake_templated_types_two_arguments
    typedef *= "<"
    typedef *= build_polymake_type(type)
    typedef *= ","
    typedef *= build_polymake_type(type)
    typedef *= ">"
  end

  return typedef
end

"""
    type_of(collection::PolyDBCollection, property::String)

obtain the type description of a property in its given namespace
FIXME also needs a version number
# Examples
```julia-repl
julia> db = PolyDB.polyDB();
julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.Reflexive");
julia> PolyDB.type_of(coll,"VERTICES)
polymake::common::Matrix<Rational,NonSymmetric>
```
"""
function type_of(collection::PolyDBCollection, property::String)
  coll_schema = schema(collection)
  ref = coll_schema["properties"][property]["\$ref"]
  path = rsplit(ref, "/"; limit=3)
  typedef = nothing
  if haskey(coll_schema["definitions"][path[3]], "description")
    typedef = coll_schema["definitions"][path[3]]["description"]
  else
    typedef = "polymake::"
    type = split(path[3], "-")
    typedef *= type[1] * "::"
    popfirst!(type)
    typedef *= build_polymake_type(type)
  end
  return typedef
end

end
