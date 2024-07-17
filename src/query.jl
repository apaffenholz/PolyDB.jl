"""
    find_one(collection::PolyDBCollection; query::Union{Dict,Nothing}=nothing, projection::Union{Dict,Nothing}=nothing, skip::Union{Int,Nothing}=nothing, further_options::Union{Nothing,Dict}=nothing)

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
  further_options::Union{Nothing,Dict}=nothing,
)
  if isnothing(query)
    query = Dict()
  end
  if isnothing(further_options)
    further_options = Mongoc.BSON()
  end
  if !isnothing(skip)
    further_options["skip"] = skip
  end
  if !isnothing(projection)
    further_options["projection"] = projection
  end
  return Mongoc.find_one(collection.coll, Mongoc.BSON(query); options=isnothing(further_options) ? nothing : Mongoc.BSON(further_options))
end

"""
    find(collection::PolyDBCollection; query::Union{Dict,Nothing}=nothing, projection::Union{Dict,Nothing}=nothing, skip::Union{Int,Nothing}=nothing, limit::Union{Int,Nothing}=nothing, further_options::Union{Nothing,Dict}=nothing)

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
  further_options::Union{Nothing,Dict}=nothing,
)::Mongoc.Cursor
  if isnothing(query)
    query = Dict()
  end
  if isnothing(further_options)
    further_options = Mongoc.BSON()
  end
  if !isnothing(skip)
    further_options["skip"] = skip
  end
  if !isnothing(limit)
    further_options["limit"] = limit
  end
  if !isnothing(projection)
    further_options["projection"] = projection
  end
  return Mongoc.find(collection.coll, Mongoc.BSON(query); options=isnothing(further_options) ? nothing : Mongoc.BSON(further_options))
end


"""
    aggregate(collection::PolyDBCollection, pipeline::Vector{Dict}; further_options::Union{Nothing,Dict}=nothing)

Obtain a cursor over all results in a collection matching the pipeline
# Examples
```julia-repl
julia> db = PolyDB.polyDB();
julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.01Polytopes");
julia> pl = Array([
           Dict( "\$match" => Dict( "DIM" => 4 ) ),
           Dict( "\$group" => Dict( "_id" => "\$N_EDGES", "n_polytopes" => Dict( "\$sum" => 1 ) ) ),
           Dict( "\$sort" => Dict("_id" => 1 ) ),
               Dict( "\$project" => Dict( "_id" => 0, "n_edges" => "\$_id", "n_polytopes" => "\$n_polytopes") )
       ]);
julia> cursor = PolyDB.aggegate(coll, pl)
julia> for e in cursor
for e in cur
         println(e)
       end
BSON("{ "n_edges" : 10, "n_polytopes" : 3 }")
BSON("{ "n_edges" : 13, "n_polytopes" : 3 }")
BSON("{ "n_edges" : 14, "n_polytopes" : 4 }")
BSON("{ "n_edges" : 15, "n_polytopes" : 5 }")
BSON("{ "n_edges" : 16, "n_polytopes" : 2 }")
BSON("{ "n_edges" : 17, "n_polytopes" : 3 }")
BSON("{ "n_edges" : 18, "n_polytopes" : 11 }")
BSON("{ "n_edges" : 19, "n_polytopes" : 5 }")
BSON("{ "n_edges" : 20, "n_polytopes" : 7 }")
BSON("{ "n_edges" : 21, "n_polytopes" : 9 }")
BSON("{ "n_edges" : 22, "n_polytopes" : 13 }")
BSON("{ "n_edges" : 23, "n_polytopes" : 12 }")
BSON("{ "n_edges" : 24, "n_polytopes" : 9 }")
BSON("{ "n_edges" : 25, "n_polytopes" : 16 }")
BSON("{ "n_edges" : 26, "n_polytopes" : 12 }")
BSON("{ "n_edges" : 27, "n_polytopes" : 15 }")
BSON("{ "n_edges" : 28, "n_polytopes" : 18 }")
BSON("{ "n_edges" : 29, "n_polytopes" : 9 }")
BSON("{ "n_edges" : 30, "n_polytopes" : 12 }")
BSON("{ "n_edges" : 31, "n_polytopes" : 10 }")
BSON("{ "n_edges" : 32, "n_polytopes" : 9 }")
BSON("{ "n_edges" : 33, "n_polytopes" : 8 }")
BSON("{ "n_edges" : 34, "n_polytopes" : 4 }")
BSON("{ "n_edges" : 35, "n_polytopes" : 3 }")
BSON("{ "n_edges" : 36, "n_polytopes" : 5 }")
BSON("{ "n_edges" : 37, "n_polytopes" : 2 }")
BSON("{ "n_edges" : 38, "n_polytopes" : 1 }")
```
"""
function aggregate(collection::PolyDBCollection, pipeline::Vector{Dict{String}}; further_options::Union{Nothing,Dict}=nothing)::Mongoc.Cursor
  return Mongoc.aggregate(collection.coll, Mongoc.BSON(pipeline); options=isnothing(further_options) ? nothing : Mongoc.BSON(further_options))
end

"""
    filter(collection::PolyDBCollection, String; filter::Union{Nothing,Dict}=nothing)

Obtain an array containing all differenct values of a property over all doxuments matching the filter
# Examples
```julia-repl
julia> db = PolyDB.polyDB();
julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.01Polytopes");
julia> filter = Dict("DIM"=>4, "N_FACETS"=>6);
julia> res = PolyDB.distinct(coll, "N_EDGES", filter = filter)
4-element Vector{Any}:
 13
 15
 16
 18
```
"""
function distinct(collection::PolyDBCollection, property::String; filter::Union{Dict,Nothing} = nothing)::Array
  db = collection.db.db
  query = Mongoc.BSON()
  query["distinct"] = collection.name
  query["key"] = property
  if !isnothing(filter)
    query["query"] = Mongoc.BSON(filter)
  end

  return Mongoc.command_simple(db, Mongoc.BSON(query))["values"]
end


"""
    find_one(collection::PolyDBCollection; query::Union{Dict,Nothing}=nothing, projection::Union{Dict,Nothing}=nothing, skip::Union{Int,Nothing}=nothing, further_options::Union{Nothing,Dict}=nothing)

Retrive one element from the collection that matches the given query
# Examples
```julia-repl
julia> db = PolyDB.polyDB();
julia> coll = PolyDB.get_collection(db, "Polytopes.Lattice.Reflexive");
julia> PolyDB.sample(coll, filter = Dict("N_VERTICES" => 7));
julia> typeof(p)
Mongoc.BSON
```
"""
function sample(collection::PolyDBCollection; filter::Union{Dict,Nothing} = nothing, further_options::Union{Dict,Nothing} = nothing)::Union{Mongoc.BSON, Nothing}

  if !isnothing(filter)
    pipeline = Array([
      Dict( "\$match" => filter ),
      Dict( "\$sample" => Dict( "size" => 1 ) )
    ])
  else
    pipeline = Array([
      Dict( "\$sample" => Dict( "size" => 1) )
    ])
  end

  cur = Mongoc.aggregate(collection.coll, Mongoc.BSON(pipeline); options=isnothing(further_options) ? nothing : Mongoc.BSON(further_options))
  try
    next = iterate(cur)
    return first(next)
  catch e
    return nothing
  end
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

function as_dict(
  doc::Union{Nothing, Mongoc.BSON}
)::Union{Dict{String,Any},Nothing}
  if isnothing(doc)
    return doc
  else
    return Mongoc.as_dict(doc)
  end
end

