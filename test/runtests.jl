using PolyDB
using Test

@testset "PolyDB.jl" begin
    println("Testing...")
    
    @testset verbose=true "Basic functionality" begin
        @test PolyDB.polyDB() isa PolyDBDatabase
        db = PolyDB.polyDB()

        collections = PolyDB.collections(db, filter=r"Polytopes.Combinatorial")
        @test length(collections) > 0

        collections = PolyDB.collections(db)
        @test length(collections) > 0
    end
end