from fastapi import FastAPI
from db.crud import data_field
from db.database import SessionLocal, engine, Base
from worldquant.service import WorldQuantService
from fastapi.responses import JSONResponse

Base.metadata.create_all(bind=engine)
wq_service = WorldQuantService()

import uvicorn

app = FastAPI()

@app.post("/refresh_data_fields")
async def refresh_data_fields(params):
    result = wq_service.refresh_all_datafields(params)
    if result:
        msg = 'success'
        status_code = 200
    else:
        msg = 'failure'
        status_code = 500

    return JSONResponse(
            content={"message": msg},
            status_code=status_code
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
