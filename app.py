# pass the link through db

from webscraping import webscrap
from text_cleaning import clean_text
import pandas as pd
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, BigInteger, Text, Date
)
import psycopg2
from dotenv import load_dotenv
import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
from pydantic import BaseModel
import time
load_dotenv(override=True)


app = FastAPI()
security = HTTPBearer()
CRAWL_BEARER_TOKEN = os.getenv("CRAWL_TOKEN_AUTH")


class Crawl_request(BaseModel):
    brand:str
    link:str


def df_upload(df,brands,link,engine):

    today = datetime.today().date()

    p = clean_text(df[df['tags'] == 'p']['text'].values[0])
    span = clean_text(df[df['tags'] == 'span']['text'].values[0])
    article = clean_text(df[df['tags'] == 'article']['text'].values[0])
    h1 = clean_text(df[df['tags'] == 'h1']['text'].values[0])
    h2 = clean_text(df[df['tags'] == 'h2']['text'].values[0])
    h3 = clean_text(df[df['tags'] == 'h3']['text'].values[0])

    cleaned_df = pd.DataFrame({
        'tags': ['p', 'span', 'article', 'h1', 'h2', 'h3'],
        'text': [p, span, article, h1, h2, h3],
        'date': [today] * 6,
        'link':[link] * 6

    })

    base_ns = time.time_ns()
    cleaned_df.insert(0, 'id', [base_ns + i for i in range(len(cleaned_df))])
    # define the table with id as a primary key
    metadata = MetaData()
    table = Table(
        brands, metadata,
        Column('id', BigInteger, primary_key=True),
        Column('tags', Text),
        Column('text', Text),
        Column('date', Date),
        Column('link', Text),
    )
    metadata.create_all(engine)  # this will DROP/CREATE just once

    # now append your rows without touching schema
    cleaned_df.to_sql(brands, engine, if_exists='replace', index=False)



async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> None:

    if credentials.credentials != CRAWL_BEARER_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )







@app.post("/crawl", dependencies=[Depends(verify_token)])
async def search(request: Crawl_request):

    try:

        print('scrape_before')
        df = await webscrap(request.brand,request.link)
        if df:
            print('scrape after')
            conn_string = os.getenv('SQL_CONNECTION') 
            engine = create_engine(conn_string)

            
            df_upload(df,request.brand,request.link,engine)

    

    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:  # noqa: BLE001
        print(exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {exc}",
        ) from exc



if __name__=='__main__':

    
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)

    


