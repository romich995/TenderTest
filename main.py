import celery
import requests as req
import bs4
import xmltodict
import time

def get_page(url, max_retries=10, sleep_time_seconds=3, **kwargs):
    resp = req.get(url, **kwargs)
    i = 1
    while not (200 <= resp.status_code < 300) and i < max_retries:
        print(f'Attemption number {i}: {url} {resp.status_code} {len(resp.text)}')
        time.sleep(sleep_time_seconds)
        resp = req.get(url, **kwargs)
        i += 1
    return resp


class ParseXML(celery.Task):
    name = 'ParseXML'

    def run(self, xml_url: str):
        resp = get_page(xml_url)
        if 200 <= resp.status_code < 300:
            dct = list(xmltodict.parse(resp.text).values())[0]
            try:
                publishDTInEIS = dct['commonInfo']['publishDTInEIS']
            except KeyError as err:
                publishDTInEIS = None
        else:
            publishDTInEIS = None
        print(f'{xml_url}-{publishDTInEIS}')
class ParsePage(celery.Task):
    name='ParsePage'
    def run(self, page: int):
        base_url = 'https://zakupki.gov.ru/epz/order/extendedsearch/results.html'

        resp = get_page(base_url, params={'fz44': 'on', 'pageNumber': page})

        page = bs4.BeautifulSoup(resp.text)

        for entry in page.select('.registry-entry__header'):
            a = entry.select('a[href]')[1]
            xml_endpoint = a.get('href').replace('view', 'viewXml')
            xml_url = f'https://zakupki.gov.ru{xml_endpoint}'
            ParseXML().delay(xml_url=xml_url)

#if __name__ == '__main__':
app = celery.Celery('Parsing', broker='amqp://localhost')
app.register_task(ParsePage())
app.register_task(ParseXML())
for page_number in range(1,3):
    ParsePage().delay(page=page_number)