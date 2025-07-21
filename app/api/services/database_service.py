from sqlalchemy import create_engine, inspect

def pull_schema(db_type: str, connection_string: str) -> dict:
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    schema = {
        "tables": [],
        "relationships": []
    }
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        pk_columns = inspector.get_pk_constraint(table_name)["constrained_columns"]
        fk_constraints = inspector.get_foreign_keys(table_name)
        table = {
            "name": table_name,
            "columns": [{"name": c["name"], "type": str(c["type"])} for c in columns],
            "primary_key": pk_columns,
            "foreign_keys": [{"column": fk["name"], "references": fk["referred_table"], "referred_column": fk["referred_columns"][0]} for fk in fk_constraints]
        }
        schema["tables"].append(table)
    for table in schema["tables"]:
        for fk in table.get("foreign_keys", []):
            relationship = {
                "from": table["name"],
                "to": fk["references"],
                "via": fk["column"]
            }
            schema["relationships"].append(relationship)
    return schema