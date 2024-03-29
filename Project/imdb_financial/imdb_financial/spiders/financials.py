import time

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request


class FinancialsSpider(scrapy.Spider):
    name = "financials"
    allowed_domains = ["www.imdb.com"]
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"

    df = pd.read_csv('data/film_urls.csv')
    start_urls = df.movie_url.tolist()

    def start_requests(self):
        for d in self.start_urls:
            yield Request(d,
                          callback=self.parse,
                          headers={"User-Agent": self.user_agent}
                          )

    def parse(self, response):
        yield {
            "title": response.xpath('//h1/span/text()').get(),
            "top_cast": response.xpath('//a[@data-testid="title-cast-item__actor"]/text()').getall(),
            "director": response.xpath('//ul[@class="ipc-metadata-list ipc-metadata-list--dividers-all sc-bfec09a1-8 '
                                       'iiDmgX ipc-metadata-list--base"]/li[1]//a[contains(@href, "name")]/text('
                                       ')').get(),
            "writer": response.xpath('//ul[@class="ipc-metadata-list ipc-metadata-list--dividers-all sc-bfec09a1-8 '
                                     'iiDmgX ipc-metadata-list--base"]/li[2]//a[contains(@href, "name")]/text()').get(),
            "country_of_origin": response.xpath('//a[contains(@href, "country_of_origin")]/text()').getall(),
            "language": response.xpath('//a[contains(@href, "language")]/text()').get(),
            "release_date": response.xpath('//div[@class="ipc-metadata-list-item__content-container"]//a[contains('
                                           '@href, "release")]/text()').get(),
            "production_company": response.xpath(
                '//li[@class="ipc-inline-list__item"]/a[contains(@href, "company")]/text()').getall(),
            "filming_locations": response.xpath(
                '//div[@class="ipc-metadata-list-item__content-container"]//a[contains(@href, "locations")]/text()').get(),
            "aspect_ratio": response.xpath(
                '//li[@data-testid="title-techspec_aspectratio"]/div//span/text()').get(),
            "budget_local_currency": response.xpath(
                '//li[@data-testid="title-boxoffice-budget"]//li/span/text()').get(),
            "gross_us_canada": response.xpath(
                '//li[@data-testid="title-boxoffice-grossdomestic"]//li/span/text()').get(),
            "opening_weekend_us_canada_usd": response.xpath(
                '//li[@data-testid="title-boxoffice-openingweekenddomestic"]//li[1]/span/text()').get(),
            "opening_weekend_date": response.xpath(
                '//li[@data-testid="title-boxoffice-openingweekenddomestic"]//li[2]/span/text()').get(),
            "gross_worldwide": response.xpath(
                '//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]//li/span/text()').get(),
            "movie_url": response.urljoin(response.xpath('//h3/a/@href').get()),
        }


process = CrawlerProcess(settings={
    'FEED_URI': './data/films_financials.csv',
    'FEED_FORMAT': 'csv'
})

process.crawl(FinancialsSpider)
process.start()
