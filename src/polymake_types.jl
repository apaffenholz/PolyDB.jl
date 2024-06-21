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
julia> PolyDB.type_of(coll,"VERTICES")
polymake::common::Matrix<Rational,NonSymmetric>
```
"""
function type_of(collection::PolyDBCollection, property::String)
  coll_schema = collection_schema(collection,true)
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
