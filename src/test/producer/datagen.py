import random

with open('datafiles/transactions.csv', 'r') as data:
	with open('datafiles/customers.csv', 'w') as output:
		firstLine = True
		for line in data:
			if firstLine:
				firstLine = False
				continue
			basketnum = int(line.split(',')[0].strip())
			custid = random.randrange(100000, 999999, 1)
			output.write(','.join([str(basketnum), str(custid)]) + '\n')
