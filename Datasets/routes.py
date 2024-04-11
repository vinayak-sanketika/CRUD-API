# routes.py

from fastapi import APIRouter, HTTPException
from db_connection import connect_to_database
#from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import json
from typing import List

router = APIRouter()


#this is used to validate
class DataInput(BaseModel):
    id: str
    dataset_id: str
    type: str
    name: str
    validation_config: dict
    extraction_config:dict
    dedup_config: dict
    data_schema :dict
    denorm_config :dict
    router_config :dict
    dataset_config :dict
    status :str
    tags :List[str]
    data_version: int
    created_by :str
    updated_by :str
    created_date: datetime = datetime.now()
    updated_date:datetime
    published_date:datetime = datetime.now()


#This model is used to pass to the patch() method
class PatchDataInput(BaseModel):
    dataset_id: Optional[str] = None 
    type: Optional[str] = None 
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
    
    
#get
@router.get("/v1/dataset/{id}")
def get_data(id: str):
    connection = connect_to_database()
    try:
        cur=connection.cursor()
            # Execute the SQL query to select a record with the given dataset_id
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        data = cur.fetchone()
        print(data)
        if not data:
            return{id:"Data Not Found"}
        return data
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        connection.close()


#post
@router.post("/v1/dataset")
def post_data(data: DataInput):
    connection = connect_to_database()
    try:
        cur=connection.cursor()
            #Converting the validation_config dictionary to a JSON string using json.dumps() before inserting it into the database.
        validation_config_json = json.dumps(data.validation_config)
        extraction_config_json = json.dumps(data.extraction_config)
        dedup_config_json = json.dumps(data.dedup_config)
        data_schema_json = json.dumps(data.data_schema)
        denorm_config_json = json.dumps(data.denorm_config)
        router_config_json = json.dumps(data.router_config)
        dataset_config_json = json.dumps(data.dataset_config)
        print("done")
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
        #If success return 
        return {"Id":data.id, "Inserted":"true"}
    except Exception as e:
        connection.rollback()
        print("Error", e)
        raise HTTPException(status_code=404, detail="Data not inserted")
    finally:
        connection.close()

#Put
@router.put("/v1/dataset/{id}")
def put_data(id: str, data: PatchDataInput):
    connection = connect_to_database()
    try:
        cur=connection.cursor()
            # Check if the record exists before updating
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        #print("data :",existing_data)
        if not existing_data:
            return HTTPException(status_code=404, detail={id:"Data not found"})

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
        return{"Id":data.id, "Updated":"true"}
    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        connection.close()

#Patch

@router.patch("/v1/dataset/{id}")
def patch_data(id: str, data: PatchDataInput):
    connection = connect_to_database()
    try:
        cur = connection.cursor()
        # Check if the record exists before updating
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        if not existing_data:
            return HTTPException(status_code=404, detail={id: "Data not found"})

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

        if data.dataset_id is not None and data.dataset_id != "null":
            update_query += "dataset_id = %s, "
            update_values.append(data.dataset_id)

        if data.type is not None and data.type !="null":
            update_query += "type = %s, "
            update_values.append(data.type)

        if data.name is not None and data.name !="null":
            update_query +="name = %s, "
            update_values.append(data.name)

        if validation_config_json is not None and validation_config_json !="null":
            update_query +="validation_config = %s, "
            update_values.append(validation_config_json)

        if extraction_config_json is not None and extraction_config_json !="null":
            update_query +="extraction_config = %s, "
            update_values.append(extraction_config_json)

        if dedup_config_json is not None and dedup_config_json != "null":
            update_query +="dedup_config = %s, "
            update_values.append(dedup_config_json)
        
        if data_schema_json is not None and data_schema_json !="null":
            update_query +="data_schema = %s, "
            update_values.append(data_schema_json)
        
        if denorm_config_json is not None and denorm_config_json !="null":
            update_query +="denorm_config = %s, "
            update_values.append(denorm_config_json)

        if router_config_json is not None and router_config_json !="null":
            update_query +="router_config = %s, "
            update_values.append(router_config_json)

        if dataset_config_json is not None and dataset_config_json !="null":
            update_query +="dataset_config = %s, "
            update_values.append(dataset_config_json)

        if data.status is not None and data.status !="null":
            update_query += "status = %s, "
            update_values.append(data.status)

        if data.tags is not None and data.tags !="null":
            update_query += "tags = %s, "
            update_values.append(data.tags)

        if data.data_version is not None and data.data_version !="null":
            update_query += "data_version = %s, "
            update_values.append(data.data_version)

        if data.created_by is not None and data.created_by !="null":
            update_query += "created_by = %s, "
            update_values.append(data.created_by)

        if data.updated_by is not None and data.updated_by !="null":
            update_query += "updated_by = %s, "
            update_values.append(data.updated_by)

        print("Update query:",update_query)
        print("Update values:",update_values)
        # Removing the trailing comma and space from the query
        update_query = update_query.rstrip(", ")
        
        update_query += " WHERE id = %s ;"
        update_values.append(id)

        cur.execute(update_query, update_values)
        connection.commit()
        return {"Id": id, "Updated": "true"}
    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        connection.close()


@router.delete("/v1/dataset/{id}")
def delete_data(id: str):
    connection = connect_to_database()
    try:
        cur=connection.cursor()
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        if not existing_data:
            return HTTPException(status_code=404, detail={id:"Data not found"})

            # Delete the record
        cur.execute("DELETE FROM datasets WHERE id = %s;", (id,))
        connection.commit()
        return {"ID":id,"Deleted":"true"}

    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        connection.close()

