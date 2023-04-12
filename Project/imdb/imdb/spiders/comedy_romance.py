import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_selenium import SeleniumRequest

class ComedyRomanceSpider(scrapy.Spider):
    name = "comedy_romance"

    def start_requests(self):
        yield SeleniumRequest(
            url='https://www.imdb.com/search/title/?genres=comedy,romance',
            wait_time=3,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        products = response.xpath('//div[@class="lister-item mode-advanced"]')
        for product in products:
            yield {
                "popularity": product.xpath('.//h3[@class="lister-item-header"]/span[1]/text()').get(),
                "title": product.xpath('.//h3/a[1]/text()').get(),
                "episode": product.xpath('.//h3/a[2]/text()').get(),
                "year": product.xpath('.//span[@class="lister-item-year text-muted unbold"]/text()').get(),
                "episode_year": product.xpath('.//span[@class="lister-item-year text-muted unbold"][2]/text()').get(),
                "rating": product.xpath('.//div[@class="inline-block ratings-imdb-rating"]/strong/text()').get(),
                "age": product.xpath('.//span[@class="certificate"]/text()').get(),
                "duration": product.xpath('.//span[@class="runtime"]/text()').get(),
                "genre": product.xpath('normalize-space(.//span[@class="genre"]/text())').get(),
                "votes": product.xpath('.//p[@class="sort-num_votes-visible"]/span[@name="nv"][1]/text()').get(),
                "description": product.xpath('normalize-space(.//p[@class="text-muted"]/text())').get(),
                "movie_url": response.urljoin(product.xpath('.//h3/a/@href').get()),
            }

        next_page = response.xpath('//a[@class="lister-page-next next-page"]/@href').get()
        if next_page:
            absolute_url = f"https://www.imdb.com{next_page}"
            yield SeleniumRequest(
                url=absolute_url,
                wait_time=3,
                callback=self.parse
            )


process = CrawlerProcess(settings={
    'FEED_URI': './data/comedy_romance.csv',
    'FEED_FORMAT': 'csv'
})

process.crawl(ComedyRomanceSpider)
process.start()
