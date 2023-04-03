import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess


class ImdbSpiderSpider(CrawlSpider):
    name = "imdb_spider"
    allowed_domains = ["www.imdb.com"]

    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"

    def start_requests(self):
        yield scrapy.Request(url="https://www.imdb.com/feature/genre/?ref_=nv_ch_gr", headers={"User_Agent": self.user_agent})

    rules = (
        Rule(
            LinkExtractor(restrict_xpaths=('//div[@class="image"]/a')),
            follow=True,
            process_request="set_user_agent"
        ),
        # Rule(
        #     LinkExtractor(restrict_xpaths=('//h3/a')),
        #     follow=True,
        #     process_request="set_user_agent"
        # ),
        Rule(
            LinkExtractor(restrict_xpaths=('(//a[@class="lister-page-next next-page"])[2]')),
            follow=True,
            callback='parse_item',
            process_request="set_user_agent"
        ),
    )

    def set_user_agent(self, request, response):
        request.headers["User-Agent"] = self.user_agent
        return request


    def parse_item(self, response):
        yield {
            "title": response.xpath('//h3/a/text()').get(),
            "year": response.xpath('//h3/span[2]/text()').get(),
            "age": response.xpath('//p[@class="text-muted "]/span[1]/text()').get(),
            "duration": response.xpath('//p[@class="text-muted "]/span[3]/text()').get(),
            "genre": response.xpath('//p[@class="text-muted "]/span[5]/text()').get(),
            "votes": response.xpath('//p[@class="sort-num_votes-visible"]/span[@name="nv"][1]/text()').get(),
            "description": response.xpath('//p[@class="text-muted"]/text()').get(),
            "movie_url": response.urljoin(response.xpath('//h3/a/@href').get()),
        }

process = CrawlerProcess(settings={
    'FEED_URI': './data/films.csv', 
    'FEED_FORMAT': 'csv'
})

process.crawl(ImdbSpiderSpider)
process.start()