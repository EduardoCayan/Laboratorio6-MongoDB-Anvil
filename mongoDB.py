# mongoDB.py
from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

MONGO_URI = "mongodb+srv://lab6_user:28072007R@tiendas.47cbags.mongodb.net/?appName=Tiendas"
DB_NAME = "Tienda"
ALLOWED_COLLECTIONS = {"productos", "empleados", "clientes", "ventas"}

_client: Optional[MongoClient] = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client

def get_collection(name: str) -> Collection:
    if not name or name not in ALLOWED_COLLECTIONS:
        raise ValueError(f"Colección inválida. Usa una de: {sorted(ALLOWED_COLLECTIONS)}")
    db = get_client()[DB_NAME]
    return db[name]

# ---------------- CRUD ----------------

def create_one(collection_name: str, doc: Dict[str, Any]) -> str:
    if not isinstance(doc, dict) or len(doc.keys()) < 3:
        raise ValueError("doc debe ser dict con al menos 3 campos (sin contar _id).")
    try:
        col = get_collection(collection_name)
        res = col.insert_one(doc)
        return str(res.inserted_id)
    except PyMongoError as e:
        raise RuntimeError(f"Mongo error en create_one: {e}")

def read_many(collection_name: str, query: Optional[Dict[str, Any]] = None, limit: int = 50) -> List[Dict[str, Any]]:
    if query is None:
        query = {}
    if not isinstance(query, dict):
        raise ValueError("query debe ser dict.")
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit debe ser int > 0.")
    try:
        col = get_collection(collection_name)
        docs = list(col.find(query).limit(limit))
        for d in docs:
            d["_id"] = str(d["_id"])
        return docs
    except PyMongoError as e:
        raise RuntimeError(f"Mongo error en read_many: {e}")

def update_one(collection_name: str, query: Dict[str, Any], new_values: Dict[str, Any]) -> int:
    if not isinstance(query, dict) or not query:
        raise ValueError("query debe ser dict no vacío.")
    if not isinstance(new_values, dict) or not new_values:
        raise ValueError("new_values debe ser dict no vacío.")
    try:
        col = get_collection(collection_name)
        res = col.update_one(query, {"$set": new_values})
        return int(res.modified_count)
    except PyMongoError as e:
        raise RuntimeError(f"Mongo error en update_one: {e}")

def delete_one(collection_name: str, query: Dict[str, Any]) -> int:
    if not isinstance(query, dict) or not query:
        raise ValueError("query debe ser dict no vacío.")
    try:
        col = get_collection(collection_name)
        res = col.delete_one(query)
        return int(res.deleted_count)
    except PyMongoError as e:
        raise RuntimeError(f"Mongo error en delete_one: {e}")

# ---------------- Quick test ----------------
if __name__ == "__main__":
    print("Leyendo productos (max 5):")
    prods = read_many("productos", limit=5)
    for p in prods:
        print(p)

    print("Insertando producto test...")
    new_id = create_one("productos", {"nombre":"Vinilo Test", "precio":10, "stock":1})
    print("Inserted:", new_id)

    print("Actualizando producto test...")
    print("Modified:", update_one("productos", {"nombre":"Vinilo Test"}, {"precio": 12}))

    print("Eliminando producto test...")
    print("Deleted:", delete_one("productos", {"nombre":"Vinilo Test"}))
