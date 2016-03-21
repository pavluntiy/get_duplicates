
import html2text
from collections import Counter, defaultdict

# from src import htmlparse
# from src.docreader import DocumentStreamReader
import re
import os

import mmh3
import google.protobuf as protobuf
import subprocess

import time

import numpy as np



# from src import document_pb2
import document_pb2
import struct
import sys
import string

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
    # draft = []
    for i in range(w):
    	if baskets[i]:
    		draft |= {min(baskets[i])}
            # draft.append(min(baskets[i]))

    # print draft


    return draft


def get_sets(file_list, k = 5, w = 20):

    ids = dict()
    # sets = []
    lens = defaultdict(lambda: 0)
    hashes = defaultdict(lambda: [])
    # hashes  = []

    counter = Counter()
    i = 0
    for url, doc in read_documents(file_list):
    	
        d = get_draft(doc, k, w)
        # sets.append(d)
        counter.update(d)
        # print d
        for h in d:
            hashes[h].append(i)
        lens[i] = len(d)
            # hashes.append((h, i))
        # print i, url
    	ids[i] = url
    	# if i % 100 == 0:
    	# 	print "Got sets {0} urls".format(i)
    	i += 1
    	# if i % 100 == 0:
    	# 	print i
    # print "Got sets!"
    # return sets, ids
    # total_cnt = 0
    # for k, v in counter.iteritems():
    #     if v > 1:
    #         total_cnt += v*(v - 1) / 2
            # print counter[k], len(hashes[k])    
    # print total_cnt
    # print lens
    return hashes, lens, ids

word_re = re.compile(r'\w+', flags = re.UNICODE)
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
def get_shingles(doc, k):
    words = re.findall(word_re, doc)
    words = map(lambda s: s.translate(remove_punctuation_map), words)
    result = []
    for i in xrange(len(words) - k):
		result.append(" ".join(words[i:i + k]))

    return result   




def get_similarities(file_list):
    w = 20
    k = 5
    thr = 0.75

    timestamp = time.time()
    hashes, lens, ids = get_sets(file_list, w = w, k = k)

    # jaccs = defaultdict(lambda:[])

    # print sets[480]
    # print sets[558]
    # jaccs = dict()
    # for i in range(len(sets)):
    #     for j in range(i + 1, len(sets)):
    #         if  len(sets[i] | sets[j]):
    #             jacc = len(sets[i] & sets[j]) * 1.0 / len(sets[i] | sets[j])
    #         if jacc > thr:
    #             # print ids[i], ids[j], jacc
    #             jaccs[(i, j)] = jacc

    # return


    # for key, value in ids.iteritems():
    #     print key, value

    # for hl, value in hashes.iteritems():
    #     print value
    # # print hashes
    # # print len(hashes)

    # # hashes = sorted(hashes)
    # print time.time() - timestamp
    # print "Got hashes!"

    os.system("mkdir little")
    os.system("mkdir tmp")
    # little = open("./little/little.csv", "w")
    
    # # # for 
    # # # counter = Counter()
    cnt = 0
    tmp = []
    total_cnt = 0
    for _, h in hashes.iteritems():

        # little = open("little.csv", "w")
        total_cnt += len(h) * (len(h) - 1) / 2 
        for i in range(len(h)):
            for j in range(i + 1, len(h)):
                if cnt % 1000000 == 0:

                    if cnt != 0:
                        # print "Reset", cnt 
                        tmp = sorted(tmp)
                        for it in tmp:
                            little.write("{0} {1}\n".format(it[0], it[1]))
                        del tmp
                        tmp = []
                        little.close()
                    little = open("./little/little_{0}.csv".format(cnt), "w")


                # counter[(h[i], h[j])] += 1
                # counter[(h[j], h[i])] += 1
                # little.write("{0} {1}\n".format(h[i], h[j]))
                # little.write("{1} {0}\n".format(h[i], h[j]))
                # print (h[i], h[j])
                tmp.append((h[i], h[j]))
                # tmp.append((h[j], h[i]))
                cnt += 1

    # print total_cnt
    if tmp != []:
        tmp = sorted(tmp)
        for it in tmp:
            little.write("{0} {1}\n".format(it[0], it[1]))
        # del tmp
        little.close()

    # little.close()
    # print "Total pairs:", cnt
    # print "Going to sort!"

    # os.system("split -l 10000 ./little/little.csv ./little/little_part")
    # os.system(" sort --batch-size=10 --temporary-directory=./little --buffer-size=1000000000 -k1,1n -k2,2n  --parallel=16 -o sorted.csv ./little/little_part* ")
    # os.system(" c")
    os.system(" sort -m -k1,1n -k2,2n --buffer-size=1500000 --temporary-directory=./tmp --parallel=16 -o./sorted.csv ./little/little_*.csv")

    # ld = np.loadtxt("sorted.csv")

    # print ld.shape
    os.system("rm -rf little")
    os.system("rm -rf tmp")
    # print "Sorted", time.time() - timestamp
    f = open("sorted.csv", "r")

    # pr1 = None
    # pr2 = None
    cnt = 1
    result = []
    line = f.readline()
    pr1, pr2 = map(int, line.strip().split(' '))
    # pr1, pr2 = tmp[0]
    line = f.readline()
    iter_cnt = 0
    while line != "":

        cur1, cur2 = map(int, line.strip().split(' '))
        # cur1, cur2 = tmp[iter_cnt]
        # del line
        # print cur1, cur2
        if cur1 == pr1 and cur2 == pr2:
            # if cur1 != cur2:
            cnt += 1
        else:
            # print cur1, cur2
            # if pr1 is None:
            #     pr1 = cur1
            #     pr2 = cur2
            #     continue

            # print cnt
            # print lens[i], lens[j]
            if lens[pr1] + lens[pr2] - cnt:
                jacc = cnt * 1.0/(lens[pr1] + lens[pr2] - cnt)
            else:
                jacc = 1.0
            # print cnt, lens[pr1], lens[pr2]
            # jacc = cnt * 1.0/w
            # print jacc
            # if cnt > 0:
                # print jacc, cnt
            if jacc > thr:
                # result.append((pr1, pr2, jacc))
                pass
                # print pr1, pr2, cnt, joinacc
                # r = (pr1, pr2)
                # if r in jaccs:
                #     # if jaccs[(ids[pr1], ids[pr2])] != jacc:
                #     #      raise ValueError((ids[pr1], ids[pr2]), jacc)
                #     del jaccs[r]
                # else:
                #     raise ValueError(r, jacc)
                print ids[pr1], ids[pr2], jacc

            # iter_cnt += cnt
            cnt = 1

        pr1 = cur1
        pr2 = cur2
        line = f.readline()
        iter_cnt += 1
        # print pr1, pr2

    if lens[pr1] + lens[pr2] - cnt:
        jacc = cnt * 1.0/(lens[pr1] + lens[pr2] - cnt)
    else:
        jacc = 1.0
    if jacc > thr:
            print ids[pr1], ids[pr2], jacc
    # iter_cnt += cnt
#
    # print jaccs

    # print "Loaded", iter_cnt
  
    # for key, value in counter.iteritems():
    #     jacc = value * 1.0/(value + 2 * (w - value))
    #     if jacc > thr:
    #         result.append((key[0], key[1], jacc))


                
    # # print len(sets)
    # result = []	
    # # print "Going to find similarities"
    # total = 0
    # for i in xrange(len(sets)):
    # 	for j in xrange(i + 1, len(sets)):
    # 		if len(sets[i]) == 0 and len(sets[j]) == 0:
    # 			continue
    # 		# if total % 10000 == 0:
    # 			# print "Processed {0} of {1}".format(total,  len(sets)**2)

    # 		jacc = len(sets[i] & sets[j]) * 1.0 / len(sets[i] | sets[j])
    # 		if jacc > 0.75:
    # 			result.append((i, j, jacc))

    # 		total += 1


    # print "Found {0} similarities in {1} seconds".format(len(result), time.time() - timestamp)
    # result = [(ids[it[0]], ids[it[1]], it[2]) for it in result]
    # return result


def process(file_list):
    get_similarities(file_list)

    # for it in result:
    #     print "{0} {1} {2}".format(it[0], it[1], it[2])

    # print len(result)


def main():
	path = "./dataset"
	if len(sys.argv) == 1:
		file_list = [os.path.join(path, f) for f in os.listdir(path)]

	else:
		file_list = sys.argv[1:]
	process(file_list)

if __name__ == "__main__":
	main()




