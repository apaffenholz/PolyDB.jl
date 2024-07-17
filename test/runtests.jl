using PolyDB
using Mongoc
using Test

@testset "PolyDB.jl" begin
    println("Testing...")

    @testset verbose=true "Connection and Info" begin
        @test PolyDB.polyDB() isa PolyDBDatabase
        db = PolyDB.polyDB()

        collections = PolyDB.collections(db, filter=r"Polytopes.Combinatorial")
        @test length(collections) > 0

        collections = PolyDB.collections(db)
        @test length(collections) > 0
    end

    @testset verbose=true "Query" begin
      @test PolyDB.polyDB() isa PolyDBDatabase
      db = PolyDB.polyDB()
      try
          @test Mongoc.ping(db.db.client)["ok"] == 1
      catch
          @test "not" == "connected"
      end
      collection = get_collection(db,"Polytopes.Lattice.SmoothReflexive")
      @test collection isa PolyDBCollection
      query = Dict("DIM" => 3, "N_VERTICES" => 8)
      opts_success  = Dict("skip"=>3)
      opts_nothing  = Dict("skip"=>13)

      @test find(collection, query=query) isa Mongoc.Cursor{Mongoc.Collection}
  end
end
