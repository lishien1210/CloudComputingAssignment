       
from mrjob.job import MRJob
from mrjob.step import MRStep
class WordCount(MRJob):
def steps(self):
return [
 MRStep(mapper=self.mapper_get_word,
 reducer=self.reducer_count_word)
]
def mapper_get_word(self, _, line):
( ) = line.split('\t')
yield word, 1
def reducer_count_ratings(self, key, values):
yield key, sum(values)
if __name__ == '__main__':
RatingsBreakdown.run()
