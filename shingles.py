
import html2text
from collections import Counter

from src import htmlparse
from src.docreader import DocumentStreamReader
import re
import os

import mmh3
import google.protobuf as protobuf
import subprocess

import time



from src import document_pb2
import struct
import sys

class DocumentStreamReader:
    def __init__(self, stream):
        self.stream = stream

    def __iter__(self):
        while True:
            sb = self.stream.read(4)
            if sb == '':
                return

            size = struct.unpack('i', sb)[0]
            msg = self.stream.read(size)
            doc = document_pb2.document()
            doc.ParseFromString(msg)
            yield doc




def read_documents(file_list):

	# files = [os.path.join(path, f) for f in file_list]
	zcat = subprocess.Popen(["zcat"] + file_list, stdout=subprocess.PIPE)
	reader = DocumentStreamReader(zcat.stdout)
	for document in reader:
		yield document.url, document.text


def get_draft(doc, k, w):
	baskets = [[] for i in range(w)]
	for shingle in get_shingles(doc, k = 5):
		h = mmh3.hash(shingle.encode("utf-8")) 
		baskets[h % w].append(h)

	draft = set()
	for i in range(w):
		if baskets[i]:
			draft |= {min(baskets[i])}

	
	return draft


def get_sets(file_list, k = 5, w = 20):

	ids = dict()
	sets = []
	i = 0
	for url, doc in read_documents(file_list):
		sets.append(get_draft(doc, k, w))
		ids[i] = url
		# if i % 100 == 0:
		# 	print "Got sets {0} urls".format(i)
		i += 1
		# if i % 100 == 0:
		# 	print i
	# print "Got sets!"
	return sets, ids

word_re = re.compile(r'\w+', flags = re.UNICODE)
def get_shingles(doc, k):
	words = re.findall(word_re, doc)
	for i in xrange(len(words) - k):
		yield " ".join(words[i:i + k])




def get_similarities(file_list):
	timestamp = time.time()
	sets, ids = get_sets(file_list)

	result = []	
	# print "Going to find similarities"
	total = 0
	for i in xrange(len(sets)):
		for j in xrange(i + 1, len(sets)):
			if len(sets[i]) == 0 and len(sets[j]) == 0:
				continue
			# if total % 10000 == 0:
				# print "Processed {0} of {1}".format(total,  len(sets)**2)

			jacc = len(sets[i] & sets[j]) * 1.0 / len(sets[i] | sets[j])
			if jacc > 0.6:
				result.append((i, j, jacc))

			total += 1


	# print "Found {0} similarities in {1} seconds".format(len(result), time.time() - timestamp)
	result = [(ids[it[0]], ids[it[1]], it[2]) for it in result]
	return result


def process(file_list):
	result = get_similarities(file_list)

	for it in result:
		# print "'{0}' ~ '{1}' == {2}".format(it[0], it[1], it[2])
		print "{0} {1} {2}".format(it[0], it[1], it[2])



def main():
	path = "./dataset"
	if len(sys.argv) == 1:
		file_list = [os.path.join(path, f) for f in os.listdir(path)]

	else:
		file_list = [sys.argv[1]]
	process(file_list)

if __name__ == "__main__":
	main()




