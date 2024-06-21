push!(LOAD_PATH,"../src/")

using Documenter, PolyDB, DocumenterTools, Changelog

makedocs(
    modules = [PolyDB],
    format=Documenter.HTML(
        prettyurls = !("local" in ARGS),
        assets = ["assets/favicon.ico"],
        highlights = ["yaml"],
        ansicolor = true,
        repolink = "https://github.com/apaffenholz/PolyDB.jl"
    ),
    sitename="PolyDB.jl", 
    authors = "Andreas Paffenholz",
)
