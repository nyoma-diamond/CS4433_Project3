import sys
import random
import string

cust_count = 50000
purch_count = 5000000

customers = open("output/CUSTOMERS.csv", "w")
purchases = open("output/PURCHASES.csv", "w")

# customers.write("ID,Name,Age,CountryCode,Salary\n")
# purchases.write("TransID,CustID,TransTotal,TransNumItems,TransDesc\n")

for c in range(1, cust_count):
    customers.write(f"{c},{''.join(random.choice(string.ascii_letters) for i in range(random.randint(10,20)))},{random.randint(18,100)},{random.randint(1,500)},{random.uniform(100,10000000)}\n")

for p in range(1, purch_count):
    purchases.write(f"{p},{random.randint(1,cust_count)},{random.uniform(10,2000)},{random.randint(1,15)},{''.join(random.choice(string.ascii_letters) for i in range(random.randint(20,50)))}\n")

customers.close()
purchases.close()
