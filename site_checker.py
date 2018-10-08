import asyncio
import aiohttp
import json
from lxml import html
import lxml
from aiopg.sa import create_engine
from db_settings import connection, parse_results, meta_domains, engine
import time
import async_timeout
start_time = time.time()

result = []


async def write_to_db(res):
    async with create_engine(**connection) as engine:
        async with engine.acquire() as conn:
            for data in res:
                await conn.execute(meta_domains.update().where(meta_domains.c.id == data['source_id']).
                                   values(checked=True))
                await conn.execute(parse_results.insert().values(**data))


async def write_error(res):
    async with create_engine(**connection) as engine:
        async with engine.acquire() as conn:
                await conn.execute(meta_domains.update().where(meta_domains.c.id == res['source_id']).
                                   values(checked=True))
                await conn.execute(parse_results.insert().values(**res))


async def exception_handler(domain, ex_id, error):
    exception_data = {
        'response_domain': domain,
        'response_code': error,
        'title': 'None',
        'description': 'None',
        'response_headers': 'None',
        'response_body': 'None',
        'source_id': ex_id,
        'catalog_domain': domain
    }
    await write_error(exception_data)


async def parse_site(qu):
    while qu.qsize() > 0:
        site = await qu.get()
        data = {}
        domain = site[2]

        async with aiohttp.ClientSession() as session:
            try:
                with async_timeout.timeout(60):
                    async with session.get(domain) as response:
                        code = await response.text()
                        body = code
                        code = html.fromstring(code.lower())
                        data['response_domain'] = response.host
                        data['response_code'] = response.status
                        if str(response.url).startswith('https://'):
                            data['ssl'] = True
                        try:
                            data['title'] = code.xpath('//title/text()')[0].strip()
                        except Exception:
                            data['title'] = 'None'
                        try:
                            data['description'] = code.xpath('//meta[@name="description"]/@content')[0].strip()
                        except Exception:
                            data['description'] = 'None'
                        data['response_headers'] = json.dumps(dict(response.headers))
                        data['response_body'] = json.dumps(body)
                        data['source_id'] = site[0]
                        data['catalog_domain'] = domain
                        result.append(data)
                        print(domain, 'good')

            except aiohttp.ClientConnectorCertificateError:
                print(domain, 'ClientConnectorCertificateError')
                await exception_handler(domain, site[0], 'ClientConnectorCertificateError')
            except aiohttp.ClientConnectorError:
                print(domain, ' ClientConnectorError')
                await exception_handler(domain, site[0], 'ClientConnectorError')
            except UnicodeError:
                print(domain, ' UnicodeError')
                await exception_handler(domain, site[0], 'UnicodeError')
            except asyncio.TimeoutError:
                print(domain, ' TimeoutError')
                await exception_handler(domain, site[0], 'TimeoutError')
            except ValueError:
                print(domain, ' ValueError')
                await exception_handler(domain, site[0], 'ValueError')
            except aiohttp.ServerDisconnectedError:
                print(domain, ' ServerDisconnectedError')
                await exception_handler(domain, site[0], 'ServerDisconnectedError')
            except aiohttp.ClientOSError:
                print(domain, ' ClientOSError')
                await exception_handler(domain, site[0], 'ClientOSError')
            except lxml.etree.ParserError:
                print(domain, ' ParserError')
                await exception_handler(domain, site[0], 'ClientOSError')
            except Exception as e:
                print(domain, type(e))


async def main():
    queue = asyncio.Queue()
    tasks = []
    conn = engine.connect()
    urls = conn.execute(meta_domains.select().where(meta_domains.c.checked == False).limit(1000))
    for url in list(urls):
        await queue.put(url)
    print(queue.qsize())
    for _ in range(10):
        task = asyncio.Task(parse_site(queue))
        tasks.append(task)

    await asyncio.gather(*tasks)

asyncio.run(main())
print("--- %s seconds ---" % (time.time() - start_time))

