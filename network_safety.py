import asyncio
import aiohttp

async def validate_proxy(proxy):
    url = "http://example.com"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, proxy=f"http://{proxy}", timeout=5) as response:
                if response.status == 200:
                    return True
        except Exception as e:
            pass
    return False

async def main():
    proxies = ["127.0.0.1:8080", "127.0.0.1:8081"]
    tasks = [validate_proxy(proxy) for proxy in proxies]
    results = await asyncio.gather(*tasks)
    for proxy, is_valid in zip(proxies, results):
        print(f"Proxy {proxy} is valid: {is_valid}")

if __name__ == "__main__":
    asyncio.run(main())
