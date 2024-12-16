
from mrjob.job import MRJob

import re
import sys

class StockAnalysis(MRJob):

   def mapper(self, key, value):
      date, apple_open, samsung_open = value.split(',')
      #print(value, file=sys.stderr)
      year = date[:4]
      month = date[5:7]
      if (month=='10' or month=='11' or month=='12'):
         apple_key = 'apple_%s' % year
         samsung_key = 'samsung_%s' % year
         yield(apple_key, float(apple_open))
         yield(samsung_key, float(samsung_open))
      
   def reducer(self, key, values):
      yield(key, max(values))

if __name__ == '__main__':
   StockAnalysis.run()
