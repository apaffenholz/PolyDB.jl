"""
    schema(collection::PolyDBCollection,raw::Bool)

obtain the JSON schema for a collection
"""
function collection_schema(collection::PolyDBCollection, raw::Bool=false)::Union{Schema,Mongoc.BSON}
  res = Mongoc.find_one(collection.info_coll, Mongoc.BSON("""{ "_id" : "schema.2.1" }"""))
  schema = replace_underscores(res["schema"])

  if raw
    return schema
  else
    return Schema(Mongoc.as_dict(schema))
  end
end
