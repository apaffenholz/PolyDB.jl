
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
  if isnothing(further_options)
    further_options = Mongoc.BSON()
  end
  further_options["authorizedCollections"] = true
  further_options["nameOnly"] = true

  query_filter = "^_sectionInfo\\."
  further_options["filter"] = Dict("name" => Dict("\$regex" => query_filter))
  all_sections = Mongoc.get_collection_names(db.db; options=further_options)
  all_sections = map((x) -> chop(x; head=13, tail=0), all_sections)

  sections = Vector{String}()
  if !isnothing(filter)
    for e in all_sections
      if !isnothing(match(filter, e, 1))
        push!(sections, e)
      end
    end
  else
    sections = all_sections
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
  if isnothing(further_options)
    further_options = Mongoc.BSON()
  end
  further_options["authorizedCollections"] = true
  further_options["nameOnly"] = true

  query_filter = "^_sectionInfo\\."
  further_options["filter"] = Dict("name" => Dict("\$regex" => query_filter))
  all_sections = Mongoc.get_collection_names(db.db; options=further_options)
  all_sections = map((x) -> chop(x; head=13, tail=0), all_sections)

  if recursive
    if isnothing(filter)
      filter = r"^[a-zA-Z0-9_-]+$"
    else
      filter *= r"[a-zA-Z0-9_-]*$"
    end
  end

  section_names = Vector{String}()
  if !isnothing(filter)
    for e in all_sections
      if !isnothing(match(filter, e, 1))
        push!(section_names, e)
      end
    end
  end

  for e in section_names
    filter!(f -> (isnothing(match(Regex(e * "."), f, 1))), section_names)
  end

  sections = []

  for e in section_names
    section_dict = Dict{String,Any}()
    section_dict["name"] = e
    if with_collections
      section_dict["collections"] = map(
        (x) -> chop(x; head=length(e) + 1, tail=0),
        collections(db; filter=e * r"\.[A-Za-z0-9_-]+$"),
      )
    end
    if recursive
      subsections = []
      for f in all_sections
        if !isnothing(match(Regex(e * "."), f, 1))
          subsection_string = chop(f; head=length(e) + 1, tail=0)
          subsection_dict = Dict(
            "name" => subsection_string, "subsections" => Dict{String,Any}()
          )
          if with_collections
            subsection_dict["collections"] = map(
              (x) -> chop(x; head=length(f) + 1, tail=0), collections(db; filter=Regex(f))
            )
          end
          push!(subsections,subsection_dict)
        end
      end
    end
    section_dict["subsections"] = subsections
    push!(sections, section_dict)
  end

  return sections
end