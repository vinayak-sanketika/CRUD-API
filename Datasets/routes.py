# routes.py

from fastapi import APIRouter, HTTPException
from db_connection import connect_to_database
#from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import json
from typing import List, Optional

router = APIRouter()


#this is used to validate
class DataInput(BaseModel):
    id: str
    dataset_id: Optional[str] = None 
    type: str
    name: Optional[str] = None
    validation_config: Optional[dict] =None
    extraction_config:Optional[dict] =None
    dedup_config: Optional[dict] =None
    data_schema :Optional[dict] =None
    denorm_config :Optional[dict] =None
    router_config :Optional[dict] =None
    dataset_config :Optional[dict] =None
    status :Optional[str] = None
    tags :Optional[List[str]] =None
    data_version: Optional[int]=None
    created_by :Optional[str] = None
    updated_by :Optional[str] = None
    created_date: datetime= datetime.now()
    updated_date: datetime = None
    published_date:datetime = datetime.now



class PatchDataInput(BaseModel):
    dataset_id: Optional[str] = None 
    type: Optional[str] = None #
    name: Optional[str] = None
    validation_config: Optional[dict] =None
    extraction_config:Optional[dict] =None
    dedup_config: Optional[dict] =None
    data_schema :Optional[dict] =None
    denorm_config :Optional[dict] =None
    router_config :Optional[dict] =None
    dataset_config :Optional[dict] =None
    status :Optional[str] = None
    tags :Optional[List[str]] =None
    data_version: Optional[int]=None
    created_by :Optional[str] = None
    updated_by :Optional[str] = None
    

    

@router.get("/v1/dataset/{id}")
def get_data(id: str):
    connection = connect_to_database()
    try:
        current_time=datetime.now()
        cur=connection.cursor()
            # Execute the SQL query to select a record with the given dataset_id
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        data = cur.fetchone()
        print(data)
        if not data:
            return HTTPException(status_code=404, 
                                 detail="Data not found",
                                 headers={
                                            "id": "api.dataset.get",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"DATASET_NOT_FOUND",
                                                        "status": "Failed",
                                                        "errmsg": "Data not found"
                                                        },
                                                "responseCode": "NOT_FOUND",
                                                "result": {
                                                            "id":id
                                                        }
                                            })
        return data
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, 
                            detail="Internal Server Error",
                            headers={
                                            "id": "api.dataset.get",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"Internal_Server_Error",
                                                        "status": "Failed",
                                                        "errmsg": e
                                                        },
                                                "responseCode": "SERVER_ERROR",
                                                "result": {
                                                            "id":id
                                                        }
                                            }
                            )
    finally:
        connection.close()



@router.post("/v1/dataset")
def post_data(data: DataInput):
    connection = connect_to_database()
    try:
        current_time=datetime.now()
        cur=connection.cursor()
            #Converting the validation_config dictionary to a JSON string using json.dumps() before inserting it into the database.
        validation_config_json = json.dumps(data.validation_config)
        extraction_config_json = json.dumps(data.extraction_config)
        dedup_config_json = json.dumps(data.dedup_config)
        data_schema_json = json.dumps(data.data_schema)
        denorm_config_json = json.dumps(data.denorm_config)
        router_config_json = json.dumps(data.router_config)
        dataset_config_json = json.dumps(data.dataset_config)
        
        if data.id or data.type  is (None or "string") :
            return HTTPException(status_code=404, 
                                 detail="Enter the required field value", 
                                 headers={
                                            "id": "api.dataset.create",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"Insert required",
                                                        "status": "Failed",
                                                        "errmsg": "Data not inserted"
                                                        },
                                                "responseCode": "Bad_Request",
                                                "result": {
                                                            "id": data.id
                                                        }
                                            })
        cur.execute("INSERT INTO datasets (id, dataset_id, type, name, validation_config,extraction_config,dedup_config,data_schema,denorm_config,router_config,dataset_config, status,tags,data_version,created_by,updated_by, created_date,updated_date,published_date) VALUES (%s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s,%s,%s,%s, %s, %s,%s,%s);", 
        (   data.id,
            data.dataset_id,
            data.type, 
            data.name, 
            validation_config_json, 
            extraction_config_json, 
            dedup_config_json, 
            data_schema_json, 
            denorm_config_json, 
            router_config_json, 
            dataset_config_json, 
            data.status, 
            data.tags, 
            data.data_version, 
            data.created_by, 
            data.updated_by, 
            data.created_date, 
            data.updated_date, 
            data.published_date))
        
        connection.commit()
        return {"id": "api.dataset.create",
                "ver": "1.0",
                "ts": current_time,
                "params": {
                            "err":"null",
                            "status": "Successful",
                            "errmsg": "null"
                            },
                    "responseCode": "ok",
                    "result": {
                                "id": data.id
                            }
                }
    except Exception as e:
        connection.rollback()
        print("Error", e)
        return HTTPException(status_code=500, 
                             detail="Internal Server Error", 
                             headers={
                                 "id": "api.dataset.create",
                                "ver": "1.0",
                                "ts": current_time,
                                "params": {
                                            "err":"Internal_Server_Error",
                                            "status": "Failed",
                                            "errmsg": "Duplicate_Key_Value"
                                            },
                                    "responseCode": "Server_Error",
                                    "result": {
                                                "id": data.id
                                            }
                                })
    finally:
        connection.close()


@router.put("/v1/dataset/{id}")
def put_data(id: str, data: PatchDataInput):
    connection = connect_to_database()
    try:
        current_time=datetime.now()
        cur=connection.cursor()
            # Check if the record exists before updating
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        #print("data :",existing_data)
        if not existing_data:
            return HTTPException(status_code=404, 
                                 detail={id:"Data not found"},
                                 headers={
                                            "id": "api.dataset.put",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"DATASET_NOT_FOUND",
                                                        "status": "Failed",
                                                        "errmsg": "Data not found"
                                                        },
                                                "responseCode": "NOT_FOUND",
                                                "result": {
                                                            "id":id
                                                        }
                                            }
                                 )

            # Update the record
        validation_config_json = json.dumps(data.validation_config)
        extraction_config_json = json.dumps(data.extraction_config)
        dedup_config_json = json.dumps(data.dedup_config)
        data_schema_json = json.dumps(data.data_schema)
        denorm_config_json = json.dumps(data.denorm_config)
        router_config_json = json.dumps(data.router_config)
        dataset_config_json = json.dumps(data.dataset_config)
        cur.execute("UPDATE datasets SET dataset_id = %s, type = %s, name = %s, validation_config = %s, extraction_config=%s,dedup_config=%s,data_schema=%s,denorm_config=%s,router_config=%s,dataset_config=%s,status=%s,tags=%s,data_version=%s,created_by=%s,updated_by=%s  WHERE id = %s;",
        (   data.dataset_id, 
            data.type, 
            data.name, 
            validation_config_json,
            extraction_config_json,
            dedup_config_json,
            data_schema_json,
            denorm_config_json,
            router_config_json,
            dataset_config_json,
            data.status,
            data.tags,
            data.data_version,
            data.created_by,
            data.updated_by, 
            id))
        connection.commit()
        return {"id": "api.dataset.put",
                "ver": "1.0",
                "ts": current_time,
                "params": {
                            "err":"null",
                            "status": "Successful",
                            "errmsg": "null"
                            },
                    "responseCode": "ok",
                    "result": {
                                "id":id
                            }
                }
    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500,
                             detail="Internal Server Error",
                             headers={
                                            "id": "api.dataset.put",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"Internal server error",
                                                        "status": "Failed",
                                                        "errmsg": e
                                                        },
                                                "responseCode": "Internal_Server_Error",
                                                "result": {
                                                            "id":id
                                                        }
                                            }
                             )
    finally:
        connection.close()

#Patch

@router.patch("/v1/dataset/{id}")
def patch_data(id: str, data: PatchDataInput):
    connection = connect_to_database()
    try:
        current_time=datetime.now()
        cur = connection.cursor()
        # Check if the record exists before updating
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        if not existing_data:
            return HTTPException(status_code=404, 
                                 detail={id: "Data not found"},
                                 headers={
                                            "id": "api.dataset.patch",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"DATASET_NOT_FOUND",
                                                        "status": "Failed",
                                                        "errmsg": "Data not found"
                                                        },
                                                "responseCode": "NOT_FOUND",
                                                "result": {
                                                            "id":id
                                                        }
                                            })

        # Update the record
        validation_config_json = json.dumps(data.validation_config)
        extraction_config_json = json.dumps(data.extraction_config)
        dedup_config_json = json.dumps(data.dedup_config)
        data_schema_json = json.dumps(data.data_schema)
        denorm_config_json = json.dumps(data.denorm_config)
        router_config_json = json.dumps(data.router_config)
        dataset_config_json = json.dumps(data.dataset_config)
        
        update_query = "UPDATE datasets SET "
        update_values = []

        # Dictionary containing fields and their values
        fields = {
            "dataset_id": data.dataset_id,
            "type": data.type,
            "name": data.name,
            "validation_config": validation_config_json,
            "extraction_config": extraction_config_json,
            "dedup_config": dedup_config_json,
            "data_schema": data_schema_json,
            "denorm_config": denorm_config_json,
            "router_config": router_config_json,
            "dataset_config": dataset_config_json,
            "status": data.status,
            "tags": data.tags,
            "data_version": data.data_version,
            "created_by": data.created_by,
            "updated_by": data.updated_by
        }
        for field,value in fields.items():
            if value is not None and value !="null":
                update_query += f"{field} = %s, "
                update_values.append(value)

       
        # Removing the trailing comma and space from the query
        update_query = update_query.rstrip(", ")

        # Add WHERE clause for the specific id
        update_query += " WHERE id = %s ;"
        update_values.append(id)

        # Execute the update query with the provided values
        cur.execute(update_query, update_values)
        connection.commit()
        return {
                "id": "api.dataset.patch",
                "ver": "1.0",
                "ts": current_time,
                "params": {
                            "err":"null",
                            "status": "Successful",
                            "errmsg": "null"
                            },
                "responseCode": "ok",
                "result": {
                 "id":id
                        }
                }
    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500, 
                            detail="Internal Server Error",
                            headers={
                                            "id": "api.dataset.patch",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"DATABASE_ERROR",
                                                        "status": "Failed",
                                                        "errmsg": e
                                                        },
                                                "responseCode": "Internal_Server_error",
                                                "result": {
                                                            "id":id
                                                        }
                                            }
                            
                            )
    finally:
        connection.close()


@router.delete("/v1/dataset/{id}")
def delete_data(id: str):
    connection = connect_to_database()
    try:
        current_time=datetime.now()
        cur=connection.cursor()
            # Check if the record exists before deleting
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        if not existing_data:
            return HTTPException(status_code=404, 
                                 detail={id:"Data not found"}, 
                                 headers={
                                            "id": "api.dataset.delete",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"DATASET_NOT_FOUND",
                                                        "status": "Failed",
                                                        "errmsg": "Data not found"
                                                        },
                                                "responseCode": "NOT_FOUND",
                                                "result": {
                                                            "id":id
                                                        }
                                            })

            # Delete the record
        cur.execute("DELETE FROM datasets WHERE id = %s;", (id,))
        connection.commit()
        return {
                "id": "api.dataset.delete",
                 "ver": "1.0",
                 "ts": current_time,
                "params": {
                            "err":"null",
                            "status": "Failed",
                            "errmsg": "null"
                            },
                "responseCode": "ok",
                "result": {
                            "id":id
                        }
                }

    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500, 
                            detail="Internal Server Error",
                            headers={
                                            "id": "api.dataset.delete",
                                            "ver": "1.0",
                                            "ts": current_time,
                                            "params": {
                                                        "err":"DATABASE_ERROR",
                                                        "status": "Failed",
                                                        "errmsg": e
                                                        },
                                                "responseCode": "Internal_Server_Error",
                                                "result": {
                                                            "id":id
                                                        }
                                            })
    finally:
        connection.close()

