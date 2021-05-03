import random

customers = {}

with open('datafiles/transactions.csv', 'r') as data:
	with open('datafiles/customers.csv', 'w') as output:
		firstLine = True
		for line in data:
			if firstLine:
				firstLine = False
				continue
			basketnum = int(line.split(',')[0].strip())
			custid = 0
			if basketnum in customers:
				custid = customers[basketnum]
			else:
				custid = random.randrange(100000, 999999, 1)
				customers[basketnum] = custid
			output.write(','.join([str(basketnum), str(custid)]) + '\n')
			print(custid)
