from db_settings import parse_results, engine
from pywhois import whois
import pywhois
from sqlalchemy import select

some = list()

conn = engine.connect()
urls = conn.execute(select([parse_results.c.id, parse_results.c.response_domain])
                    .where(parse_results.c.response_code == 'ClientConnectorError').offset(1000))
urls = list(urls)


for url in urls:
    try:
        w = whois.whois(url[1])
        exp_date = w.registrar
        if not exp_date:
            some.append(url[1])
        print(url[1], exp_date)
    except pywhois.whois.parser.PywhoisError:
        some.append(url[1])
    except Exception as err:
        print(url[1], type(err))

with open('result.txt', 'wt', encoding='utf-8') as file:
    file.write('\n'.join(str(line) for line in some))
