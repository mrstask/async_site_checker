import aiohttp
import asyncio
from lxml import html
from aiopg.sa import create_engine
from db_settings import connection, meta_domains

domains = dict()


async def write_to_db(result_domains):
    for domain in result_domains:
        async with create_engine(**connection) as engine:
            async with engine.acquire() as conn:
                domain = result_domains[domain]
                await conn.execute(meta_domains.insert().values(**domain))


async def worker(q):
    async with aiohttp.ClientSession() as session:
        while q.qsize() > 0:
            url = await q.get()  # из очереди
            if url.startswith('/'):
                url = 'http://dir.meta.ua' + url
            async with session.get(url) as response:
                code = await response.text()
                code = html.fromstring(code)
                if code.xpath('//a[@id="linkBack"]/@href'):
                    next_page = code.xpath('//a[@id="linkBack"]/@href')
                    await q.put(next_page[0])
                collect_urls = code.xpath('//a/h4/parent::a/@href')
                category_name = code.xpath('//h1/text()')[1].strip()
                collect_urls = [x.split('?')[1] for x in collect_urls]
                for single_url in collect_urls:
                        data = dict()
                        data['domain'] = single_url
                        data['category'] = category_name.strip()
                        domains[single_url] = data
                        print(len(domains))


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get('http://dir.meta.ua/ru/') as response:
            links = await response.text()
            links = html.fromstring(links)
            links = links.xpath('//a/h1/parent::a/@href')
            links = [x for x in links if 'http' not in x]
    qu = asyncio.Queue()

    for link in links:
        await qu.put(link)

    tasks = []
    for _ in range(len(links)):
        task = asyncio.Task(worker(qu))
        tasks.append(task)

    await asyncio.gather(*tasks)


asyncio.run(main())
asyncio.run(write_to_db(domains))

