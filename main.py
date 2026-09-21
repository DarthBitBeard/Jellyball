import asyncio
import aiohttp

async def fetch_data(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    urls = ["http://example.com", "http://example.org"]
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for result in results:
            print(result)

if __name__ == "__main__":
    asyncio.run(main())
