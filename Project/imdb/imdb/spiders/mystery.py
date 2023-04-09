import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_selenium import SeleniumRequest


class ComedySpider(scrapy.Spider):
    name = "mystery"

    def start_requests(self):
        yield SeleniumRequest(
            url='https://www.imdb.com/search/title/?genres=mystery',
            wait_time=3,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        products = response.xpath('//div[@class="lister-item mode-advanced"]')
        for product in products:
            yield {
                "title": response.xpath('.//h3/a/text()').get(),
                "year": response.xpath('.//span[@class="lister-item-year text-muted unbold"]/text()').get(),
                "rating": response.xpath('.//div[@class="inline-block ratings-imdb-rating"]/strong/text()').get(),
                "age": response.xpath('.//span[@class="certificate"]/text()').get(),
                "duration": response.xpath('.//span[@class="runtime"]/text()').get(),
                "genre": response.xpath('normalize-space(.//span[@class="genre"]/text())').get(),
                "votes": response.xpath('.//p[@class="sort-num_votes-visible"]/span[@name="nv"][1]/text()').get(),
                "description": response.xpath('normalize-space(.//p[@class="text-muted"]/text())').get(),
                "movie_url": response.urljoin(response.xpath('.//h3/a/@href').get()),
            }

        next_page = response.xpath('//a[@class="lister-page-next next-page"]/@href').get()
        if next_page:
            absolute_url = f"https://www.imdb.com/{next_page}"
            yield SeleniumRequest(
                url=absolute_url,
                wait_time=3,
                callback=self.parse
            )


process = CrawlerProcess(settings={
    'FEED_URI': './data/mystery.csv',
    'FEED_FORMAT': 'csv'
})

process.crawl(ComedySpider)
process.start()
