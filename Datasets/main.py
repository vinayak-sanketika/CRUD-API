# Importing the FastAPI framework
from fastapi import FastAPI

# Importing the router from the routes module
from routes import router as api_router

# Creating an instance of FastAPI
app = FastAPI()

# Including the router in the FastAPI application
app.include_router(api_router)
