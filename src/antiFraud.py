import pandas as pd
import sys, getopt
import csv
import re
from py2neo import Graph, Node, Relationship, authenticate
from neo4jrestclient.client import GraphDatabase


neo_user = "neo4j"
neo_pw = "pentiumd"
authenticate("localhost:7474", neo_user, neo_pw)
graph = Graph()
db = GraphDatabase("http://localhost:7474", username="neo4j", password="pentiumd")
 

#Users outside 4th degree friends are warned
def featureThree(args):
	print("Implementing Feature Three...")
	firstFriendFlag = False
	secondFriendFlag = False
	thirdFriendFlag = False
	fourthFriendFlag = False
	with open(args,'w+') as output:
		for index,row in dfPayment.iterrows():
			find_user1 = graph.find_one("user","num",int(row['id1']))
			find_user2 = graph.find_one("user","num",int(row['id2']))
			
			if find_user1 and find_user2:
				# Check if immediate friends
				queryFirst = 'MATCH (u) - [r] - (u2) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u2.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultFirst = db.query(queryFirst, returns = int)
				for r in resultFirst:
					if int(r[0]) > 0:
						firstFriendFlag = True
					else:
						firstFriendFlag = False

				# Check second degree friends
				querySecond = 'MATCH (u) - [r*2] - (u3) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u3.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultsSecond = db.query(querySecond, returns = int)
				for r in resultsSecond:
			
					if int(r[0]) > 0:
						secondFriendFlag = True
					else:
						secondFriendFlag = False

				#Check third degree friends
				queryThird = 'MATCH (u) - [r*3] - (u4) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u4.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultThird = db.query(queryThird, returns = int)
				for r in resultThird:
			
					if int(r[0]) > 0:
						thirdFriendFlag = True
					else:
						thirdFriendFlag = False


				#Check fourth degree friends

				queryFourth = 'MATCH (u) - [r*4] - (u5) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u5.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultsFourth = db.query(queryFourth, returns = int)
				for r in resultsFourth:
			
					if int(r[0]) > 0:
						fourthFriendFlag = True
					else:
						fourthFriendFlag = False

				if firstFriendFlag or secondFriendFlag or thirdFriendFlag or fourthFriendFlag:
					data = "trusted"
					output.write(data)
					output.write("\n")

				else:
					data = "unverified"
					output.write(data)
					output.write("\n")

						
			else:
				data = "unverified"
				output.write(data)
				output.write("\n")

	output.close()



# Check if user 1 & user 2 exists in database
# Works only if they exists

def featureTwo(args):
	print("Implementing Feature Two...")
	firstFriendFlag = False
	secondFriendFlag = False
	with open(args,'w+') as output:
		for index, row in dfPayment.iterrows():
			find_user1 = graph.find_one("user","num",int(row['id1']))
			find_user2 = graph.find_one("user","num",int(row['id2']))
			if find_user1 and find_user2:
				
				# Check if immediate friends
				queryFirst = 'MATCH (u) - [r] - (u2) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u2.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultFirst = db.query(queryFirst, returns = int)
				for r in resultFirst:
					if int(r[0]) > 0:
						firstFriendFlag = True
					else:
						firstFriendFlag = False

				# Check second degree friends
				querySecond = 'MATCH (u) - [r] - (u2)-[r1]-(u3) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u3.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultsSecond = db.query(querySecond, returns = int)
				for r in resultsSecond:
					
					if int(r[0]) > 0:
						secondFriendFlag = True
					else:
						secondFriendFlag = False

				# Based on First or Second Degree Friend flag, write the corresponding value to file
				if firstFriendFlag or secondFriendFlag:
					data = "trusted"
					output.write(data)
					output.write("\n")
						
				else:
					data = "unverified"
					output.write(data)
					output.write("\n")

			else:
				data = "unverified"
				output.write(data)
				output.write("\n")

			

	output.close()



def featureOne(stream_payment,output1):
	print("Implementing Feature One...")
	filename = stream_payment
	f = open(filename)
	csv_f = csv.reader(f)
	global dfPayment
	dfPayment = pd.DataFrame(columns=('id1','id2'))
	firstFriendFlag = False
	i=0
	for row in csv_f:
		if len(row)>2:
			if "id" not in row[1]:
				i+=1
				print(i)
			
				dfPayment.loc[len(dfPayment)] = [row[1],row[2]]

	with open(output1,'w+') as output:
		
		for index, row in dfPayment.iterrows():
			find_user1 = graph.find_one("user","num",int(row['id1']))
			find_user2 = graph.find_one("user","num",int(row['id2']))

			if find_user1 and find_user2:
				queryFirst = 'MATCH (u) - [r] - (u2) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u2.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultFirst = db.query(queryFirst, returns = int)
				for r in resultFirst:
					if int(r[0]) > 0:
						firstFriendFlag = True
					else:
						firstFriendFlag = False

				if firstFriendFlag:
					data = "trusted"
					output.write(data)
					output.write("\n")
				else:
					data = "unverified"
					output.write(data)
					output.write("\n")
			# As one or both of the user don't already exists in the system,
			# thus it will be an unverified request.
			else:
				data = "unverified"
				output.write(data)
				output.write("\n")

			# Based on first friend flag, updates the values to the file
			

	output.close()

#Create indices for faster access
def create_indices():
	global graph
	graph.run('CREATE INDEX ON :user(num)')

#Clear any pre-existing graphs
def clear_graph():
	global graph
	print("Clearing Graph...")
	clear_cypher = 'MATCH (n) DETACH DELETE n'
	graph.run(statement=clear_cypher)



def create_nodes(args):
	#Read batch_payment.csv file
	#Store file in DataFrame using pandas
	print(args)
	print("Creating Nodes...")
	filename = args
	f = open(filename)
	csv_f = csv.reader(f)
	df = pd.DataFrame(columns=('id1','id2'))
	i=0
	for row in csv_f:
	
		
		if len(row)>2:

			if "id" not in row[1]:
				i+=1
				print(i)
			
				df.loc[len(df)] = [row[1],row[2]]
			
	

	
	#Iterate Dataframe
	#Check if user-1 & user-2 nodes exists:
	# If they don't, create new nodes and their relation as "Transaction_with"
	# else connect old pre-existing nodes with new relation
	for index,row in df.iterrows():
		
		find_user1 = graph.find_one("user","num",int(row['id1']))
		find_user2 = graph.find_one("user","num",int(row['id2']))


		if find_user1:
	
			if find_user2:
	
				rel = Relationship(find_user1,"transaction_with",find_user2,relationship_type="transaction_with")
				graph.create(rel)
			else:
	
				new_user2 = Node("user", num=int(row['id2'].strip()),node_type="user")
				graph.create(new_user2)
				rel = Relationship(find_user1,"transaction_with",new_user2,relationship_type="transaction_with")
				graph.create(rel)
		elif find_user2:
	
			if not find_user1:
			
					new_user1 = Node("user", num=int(row['id1'].strip()),node_type="user")
					graph.create(new_user1)
					rel = Relationship(new_user1,"transaction_with",find_user2,relationship_type="transaction_with")
					graph.create(rel)

		else:
	
			print(row['id1'])
			new_user1 = Node("user",num=int(row['id1'].strip()),node_type="user")

			graph.create(new_user1)
			new_user2 = Node("user",num=int(row['id2'].strip()),node_type="user")
			graph.create(new_user2)
			rel = Relationship(new_user1,"transaction_with",new_user2,relationship_type="transaction_with")
			graph.create(rel)
			
		
	
	
 
		


if __name__ == "__main__":
	if len(sys.argv) <=3:
		print("Not valid arguments")
		sys.exit(0)

	try:
		opts, args = getopt.getopt(sys.argv,"hi:o:",["batch_file=","stream_file="])
		print(args)
		batch_payment = args[1]
		stream_payment = args[2]
		output1 = args[3]
		output2 = args[4]
		output3 = args[5]
	except getopt.GetoptError:
   		sys.exit(2)


	clear_graph()
	create_nodes(batch_payment)
	#featureOne(stream_payment,output1)
	#featureTwo(output2)
	#featureThree(output3)

