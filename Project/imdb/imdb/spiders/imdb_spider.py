import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess


class ImdbSpiderSpider(CrawlSpider):
    name = "imdb_spider"
    allowed_domains = ["www.imdb.com"]

    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"

    def start_requests(self):
        yield scrapy.Request(url="https://www.imdb.com/search/title/?genres=fantasy&explore=title_type,genres", headers={"User_Agent": self.user_agent})

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
            "genre": response.xpath('//p[@class="text-muted "]/span[5]/text()').get().strip(),
            "votes": response.xpath('//p[@class="sort-num_votes-visible"]/span[@name="nv"][1]/text()').get(),
            "description": response.xpath('//p[@class="text-muted"]/text()').get(),
            "team": response.xpath('//div[@class="lister-item-content"]/p[3]/a[1]/text() | //div[@class="lister-item-content"]/p[3]/a[2]/text() | //div[@class="lister-item-content"]/p[3]/a[3]/text() | //div[@class="lister-item-content"]/p[3]/a[4]/text() | //div[@class="lister-item-content"]/p[3]/a[5]/text() | //div[@class="lister-item-content"]/p[3]/a[6]/text()').get(),
            # "metascore": response.xpath('//span[@class="score-meta"]/text()').get(),
            # "oscar_nominations": response.xpath('//div[@data-testid="awards"]/ul/li/a/text()').get(),
            # "wins_and_nominations": response.xpath('//div[@data-testid="awards"]/ul/li/div/ul/li/span/text()').get(),
            # "director": response.xpath('//section[@data-testid="title-cast"]/ul/li[1]//a/text()').get(),
            # "country_of_origin": response.xpath('//div[@data-testid="title-details-section"]/ul/li[2]//a/text()').get(),
            # "production_company": response.xpath(
            #     '//div[@data-testid="title-details-section"]/ul/li[7]//div//a/text()').get(),
            # "budget_local_currency": response.xpath('(//div[@data-testid="title-boxoffice-section"]//div/ul/li/span/text())[1]').get(),
            # "gross_us_canada": response.xpath('(//div[@data-testid="title-boxoffice-section"]//div/ul/li/span/text())[2]').get(),
            # "opening_weekend_us_canada": response.xpath('(//div[@data-testid="title-boxoffice-section"]//div/ul/li/span/text())[3]').get(),
            "gross_worldwide": response.xpath('//p[@class="sort-num_votes-visible"]/span[@name="nv"][2]/@data-value').get(),
            "movie_url": response.urljoin(response.xpath('//h3/a/@href').get()),
        }

process = CrawlerProcess(settings={
    'FEED_URI': './data/films.csv', 
    'FEED_FORMAT': 'csv'
})

process.crawl(ImdbSpiderSpider)
process.start()