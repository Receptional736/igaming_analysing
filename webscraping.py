import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import re
import os
from datetime import date
from dotenv import load_dotenv
import random
from fake_useragent import UserAgent

load_dotenv()


async def fetch_p_tags(url, extract_elements):
    async with async_playwright() as p:
        # pick a random UA and referer


        # prepare a fake-UA generator
        ua = UserAgent()

        # list of plausible referers to rotate through
        REFERERS = [
            "https://www.google.com/",
            "https://www.bing.com/",
            "https://search.yahoo.com/",
            "https://www.duckduckgo.com/"
        ]
        user_agent = ua.random
        referer = random.choice(REFERERS)

        # common “human” headers
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "Referer": referer,
            "Connection": "keep-alive",
        }

        # launch browser and context with our headers
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            extra_http_headers=headers
        )
        page = await context.new_page()

        # navigate
        await page.goto(url)

        # extract <p> inner texts
        crawl_info = []
        for selector in extract_elements:
            try:
                els = await page.query_selector_all(selector)
                texts = [await el.inner_text() for el in els]
                combined = " ".join(texts).replace("\n", " ").replace("\r", " ")
                combined = re.sub(r"\s+", " ", combined).strip()
                crawl_info.append(combined)
            except Exception as e:
                print('111')
                print(e)
                crawl_info.append("")

        await browser.close()
        return crawl_info




async def webscrap(brand:str, link:str):



    links = []
    brands = []
    links.append(link)
    brands.append(brand)

    input_df = pd.DataFrame(data={
        'brands':brands,
        'Links':links
    })

   
    extract_elements = ["p", "span","article","h1","h2","h3"]


    all_dfs = []
    for brand, url in zip(input_df["brands"], input_df["Links"]):
        try:
            tags = await fetch_p_tags(url,extract_elements)

            tags_dic = {'tags':['p','span','article','h1','h2','h3'],
                        'text':tags}
            date_str = date.today().strftime("%Y_%m_%d")
            
            

            df = pd.DataFrame(tags_dic)

            df["date"] = date_str
            all_dfs.append(df)
            #df.to_csv(f"{brand}.csv", index=False)

        except Exception as e:
            print('2222')
            print(e)
            continue
    
    return all_dfs



if __name__ == '__main__':

    all_df = webscrap()

