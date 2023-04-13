from scrapy.crawler import CrawlerProcess

from action import ActionSpider
from action_comedy import ActionComedySpider
from adventure import AdventureSpider
from animation import AnimationSpider
from comedy import ComedySpider
from comedy_romance import ComedyRomanceSpider
from crime import CrimeSpider
from drama import DramaSpider
from fantasy import FantasySpider
from horror import HorrorSpider
from mystery import MysterySpider
from romance import RomanceSpider
from sci_fi import SciFiSpider
from superhero import SuperheroSpider
from thriller import ThrillerSpider

action = ActionSpider()
action_comedy = ActionComedySpider()
adventure = AdventureSpider()
animation = AnimationSpider()
comedy = ComedySpider()
comedy_romance = ComedyRomanceSpider()
crime = CrimeSpider()
drama = DramaSpider()
fantasy = FantasySpider()
horror = HorrorSpider()
mystery = MysterySpider()
romance = RomanceSpider()
sci_fi = SciFiSpider()
superhero = SuperheroSpider()
thriller = ThrillerSpider()

genres = {
    'action': action,
    'action_comedy': action_comedy,
    'adventure': adventure,
    'animation': animation,
    'comedy': comedy,
    'comedy_romance': comedy_romance,
    'crime': crime,
    'drama': drama,
    'fantasy': fantasy,
    'horror': horror,
    'mystery': mystery,
    'romance': romance,
    'sci_fi': sci_fi,
    'superhero': superhero,
    'thriller': thriller,
}

for key, value in genres:
    print(f'Crawler {key} is being run....')
    process = CrawlerProcess(settings={
        'FEED_URI': f'./data/{key}.csv',
        'FEED_FORMAT': 'csv'
    })

    process.crawl(value)
    process.start(stop_after_crawl=False)
