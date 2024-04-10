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



class PatchDataInput(BaseModel):
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
            return{"Message":"Data Not Found"}
                #return HTTPException(status_code=404, detail="Data not found")

            # Converting the fetched record into a dictionary
        result = {
                "id": data[0],
                "dataset_id": data[1],
                "type": data[2],
                "name": data[3],
                "validation_config": data[4],
                "extraction_config": data[5],
                "dedup_config": data[6],
                "data_schema": data[7],
                "denorm_config": data[8],
                "router_config": data[9],
                "dataset_config": data[10],
                "status": data[11],
                "tags": data[12],
                "data_version": data[13],
                "created_by": data[14],
                "updated_by": data[15],
                "created_date":data[16],
                "updated_date":data[17],
                "published_date": data[18],
            }
        return result
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        connection.close()



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
                                          (data.id, data.dataset_id, data.type, data.name, validation_config_json, extraction_config_json, dedup_config_json, data_schema_json, denorm_config_json, router_config_json, dataset_config_json, data.status, data.tags, data.data_version, data.created_by, data.updated_by, data.created_date, data.updated_date, data.published_date))
        connection.commit()
        #If success return 
        return {"message": "Data inserted successfully"}
    except Exception as e:
        connection.rollback()
        print("Error", e)
        raise HTTPException(status_code=404, detail="Data not inserted")
    finally:
        connection.close()


@router.patch("/v1/dataset/{id}")
def patch_data(id: str, data: PatchDataInput):
    connection = connect_to_database()
    try:
        cur=connection.cursor()
            # Check if the record exists before updating
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        if not existing_data:
            return HTTPException(status_code=404, detail="Data not found")

            # Update the record
        validation_config_json = json.dumps(data.validation_config)
        extraction_config_json = json.dumps(data.extraction_config)
        dedup_config_json = json.dumps(data.dedup_config)
        data_schema_json = json.dumps(data.data_schema)
        denorm_config_json = json.dumps(data.denorm_config)
        router_config_json = json.dumps(data.router_config)
        dataset_config_json = json.dumps(data.dataset_config)
        cur.execute("UPDATE datasets SET dataset_id = %s, type = %s, name = %s, validation_config = %s, extraction_config=%s,dedup_config=%s,data_schema=%s,denorm_config=%s,router_config=%s,dataset_config=%s,status=%s,tags=%s,data_version=%s,created_by=%s,updated_by=%s ,created_date=%s,updated_date = %s , published_date=%s WHERE id = %s;",
                        (data.dataset_id, data.type, data.name, validation_config_json,extraction_config_json,dedup_config_json,data_schema_json,denorm_config_json,router_config_json,dataset_config_json,data.status,data.tags,data.data_version,data.created_by,data.updated_by,data.created_date,data.updated_date,data.published_date, id))
        connection.commit()
        return {"Message": "Data Updated successfully"}
   # except HTTPException:
        # Re-raise HTTPException to propagate it
    #    raise
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
            # Check if the record exists before deleting
        cur.execute("SELECT * FROM datasets WHERE id = %s;", (id,))
        existing_data = cur.fetchone()
        if not existing_data:
            return HTTPException(status_code=404, detail="Data not found")

            # Delete the record
        cur.execute("DELETE FROM datasets WHERE id = %s;", (id,))
        connection.commit()
        return {"Message": "Data Deleted successfully"}
    #except HTTPException:
        # Re-raise HTTPException to propagate it
     #   raise
    except Exception as e:
        connection.rollback()
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        connection.close()

