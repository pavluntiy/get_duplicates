
import html2text
from collections import Counter

from src import htmlparse
from src.docreader import DocumentStreamReader
import re
import os

import mmh3
import google.protobuf as protobuf
import subprocess

k = 10

word_re = re.compile(r'\w+', flags = re.UNICODE)


htmls = []

shls = []
files = []

	# prefix = "./dataset"
	# for filename in os.listdir(prefix):

# from io import BufferedReader as Reader

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




def read_documents(path):

	files = [os.path.join(path, f) for f in os.listdir(path)]
	zcat = subprocess.Popen(["zcat"] + files, stdout=subprocess.PIPE)
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
		draft |= {min(baskets[i])}

	
	return draft


def get_sets(k = 5, w = 20):
	path = "./dataset"

	ids = dict()
	sets = []
	i = 0
	for url, doc in read_documents(path):
		sets.append(get_draft(doc, k, w))
		ids[i] = url
		i += 1

	return sets

def get_shingles(doc, k = 5):
	words = re.findall(word_re, doc)
	for i in xrange(len(words) - k):
		yield " ".join(words[i:i + k])



print get_sets()



# for i in xrange(len(words) - k):
# 	cur_shingle = words[i:i + k]
# 	shingles |= {" ".join(cur_shingle)}

# shls.append(shingles)


# for i in xrange(len(shls)):
# 	for j in xrange(i + 1, len(shls)):

# 		jacc = len(shls[i] & shls[j]) * 1.0 / len(shls[i] | shls[j])

# 		if jacc > 0.5:
# 			print files[i], files[j]







