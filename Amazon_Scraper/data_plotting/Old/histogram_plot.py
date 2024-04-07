import matplotlib.pyplot as plt
import chart_studio.plotly as py  # Change this line

from pymongo import MongoClient

import json

mdb = MongoClient('127.0.0.1', 27017)

print("Connection Established")

products = mdb.local.products
reviews = mdb.local.reviews

print("Collections initialised")
tups = {}

y = products.aggregate(
        [
            {"$group" : { "_id": "$asin", "brand":{'$first':"$brand"} } },
            {"$project": {"asin" : "$_id", "brand":1 ,"_id" : 1} },
        ]
    )

asin_to_brand = {}
for item in y:
    asin_to_brand[item["asin"]] = item["brand"]

z = reviews.aggregate(
        [
            {"$group" : { "_id": "$author_id", "products":{'$push':"$asin"} } },
            {"$project": {"author_id" : "$_id", "products":1 , "_id" : 1} },
        ]
)

suspicious_brands = {}
for reviewer in z:
    auth_id = reviewer['author_id']
    reviewed = reviewer['products']
    for asin in reviewed:
        brand = asin_to_brand[asin]
        try:
          tups[(auth_id, brand)] += 1
        except:
          tups[(auth_id, brand)] = 1
    if tups[(auth_id,brand)] >= 2:
        try:
            suspicious_brands[brand] += 1
        except:
            suspicious_brands[brand] = 1

x = list(suspicious_brands.keys())
y = []
for i in x:
    y.append(suspicious_brands[i])

bar = plt.bar(x,y,1/1.5,color='blue')

for r in bar:
    h = r.get_height()
    plt.text(r.get_x() + r.get_width()/2.0, h, '%d' % int(h), ha='center', va='bottom')

plt.title("User to brand coupling")
plt.savefig("plot.png")

plt.show()


with open('hist_files.txt', 'w') as f:
    for i in tups.keys():
        if tups[i] >=2 and i[1] != '':
            f.write(i[0] + ', ' + i[1] + ', ' +str(tups[i])  +'\n')
