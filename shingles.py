
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

word_re = re.compile(r'\W+')


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
		# document.body = htmlparse.parse_html(document.body)
		# yield document.url, htmlparse.parse_html(document.body)
		yield document.url, document.text



i = 0
path = "./dataset"
for doc in read_documents(path):
	i += 1
	if i % 100 == 0:
		print i
print i


# for i in xrange(len(words) - k):
# 	cur_shingle = words[i:i + k]
# 	shingles |= {" ".join(cur_shingle)}

# shls.append(shingles)


# for i in xrange(len(shls)):
# 	for j in xrange(i + 1, len(shls)):

# 		jacc = len(shls[i] & shls[j]) * 1.0 / len(shls[i] | shls[j])

# 		if jacc > 0.5:
# 			print files[i], files[j]







