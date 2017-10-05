import cPickle, gzip, numpy
import random
f = gzip.open('mnist.pkl.gz', 'rb')
train_set, valid_set, test_set = cPickle.load(f)
#x, y = train_set[0], train_set[1]
x, y = valid_set[0], valid_set[1]
n = len(x)
c = len(x[0])
print n,c
f.close()

idxes = range(0, n)
random.shuffle(idxes)

for i in range(50, 60):
	fp = open("%d_data.txt" %i, "w") 
	for ell in range(0, 1000):
		idx = idxes[1000 * (i - 50) + ell]
		fp.write("%d\t" % y[idx])
		for j in range(0, c):
			fp.write("%.4f " % x[idx][j])
		fp.write("\n")
	fp.close()
