import asyncio
import aiohttp
import time
import threading
import os
import pymongo

class AutoPoll:
  def __init__(self,interval,sources,db,collection,id = 0):
    self.id = id
    self.interval = interval
    self.messageQueue = asyncio.Queue()
    self.sources = sources
    self.db = db
    self.col = db[collection]

  async def fetch(self,session, url):
      async with session.get(url) as response:
        try:
          json_response = await response.json()
          print("Got Response from > " + url)
          await self.messageQueue.put(json_response)
        except Exception as e:
          print(e)

  async def insert_to_db(self):
    message = await self.messageQueue.get()
    self.col.insert_one(message)
    print("Message Written to db")

  async def start(self):
      async with aiohttp.ClientSession() as session:
          tasks = [self.fetch(session, url) for url in self.sources]
          await asyncio.gather(*tasks)
  
  def run(self):
    threading.Timer(self.interval, self.run).start()
    asyncio.run(self.start())
    asyncio.run(self.insert_to_db())

if __name__ == "__main__":
  client = pymongo.MongoClient(os.environ["DBURL"])
  db = client.techstax
  autoPoll = AutoPoll(interval = 5,sources = [
        "https://www.thecocktaildb.com/api/json/v1/1/random.php",
        "https://randomuser.me/api/",
        ],db = db,collection = 'base')
  autoPoll.run()
