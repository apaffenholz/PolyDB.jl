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

function as_dict(
  doc::Union{Nothing, Mongoc.BSON}
)::Union{Dict{String,Any},Nothing}
  if isnothing(doc)
    return doc
  else
    return Mongoc.as_dict(doc)
  end
end