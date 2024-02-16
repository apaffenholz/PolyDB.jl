push!(LOAD_PATH,"../src/")

using Documenter, PolyDB

makedocs(sitename="PolyDB.jl - Documentation", modules = [PolyDB])
