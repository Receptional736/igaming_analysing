# pass the link through db

from webscraping import webscrap
from text_cleaning import clean_text
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
from pydantic import BaseModel
load_dotenv(override=True)


app = FastAPI()
security = HTTPBearer()
CRAWL_BEARER_TOKEN = os.getenv("CRAWL_TOKEN_AUTH")


class Crawl_request(BaseModel):
    dummy:str


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

    cleaned_df.to_sql(f'{brands}', engine, if_exists='append', index=False) # keep old stuff 



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
        all_df,brands, links = await webscrap()
        print('scrape after')
        conn_string = os.getenv('SQL_CONNECTION') 
        engine = create_engine(conn_string)

        for i in range(len(all_df)):
            df = all_df[i]
            brand = brands[i]
            print(link)
            link = links[i]
            df_upload(df,brand,link,engine)

    

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

    


