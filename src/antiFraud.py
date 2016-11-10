import pandas as pd
import sys, getopt
import csv
import re
from py2neo import Graph, Node, Relationship, authenticate
from neo4jrestclient.client import GraphDatabase

# Initialize Neo4j graph based on assigned new password
neo_user = "neo4j"
neo_pw = "neo"
authenticate("localhost:7474", neo_user, neo_pw)
graph = Graph()
db = GraphDatabase("http://localhost:7474", username="neo4j", password="neo")
 

#Users outside 4th degree friends are warned
def featureThree(args):
	# Boolean flags to identify degree of friend
	firstFriendFlag = False
	secondFriendFlag = False
	thirdFriendFlag = False
	fourthFriendFlag = False

	#Open output file
	with open(args,'w+') as output:

		# Iterate Stream Payment dataframe
		for index,row in dfPayment.iterrows():
			#Find user nodes in graph based on their id
			find_user1 = graph.find_one("user","num",int(row['id1'].strip()))
			find_user2 = graph.find_one("user","num",int(row['id2'].strip()))
			
			#If both user exists
			if find_user1 and find_user2:

				# Check if immediate friends
				# Query returns 1 if there's a match
				queryFirst = 'MATCH (u) - [r] - (u2) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u2.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultFirst = db.query(queryFirst, returns = int)
				for r in resultFirst:
					if int(r[0]) > 0:
						firstFriendFlag = True
					else:
						firstFriendFlag = False

				# Check second degree friends
				# Query returns 1 if there's a match
				querySecond = 'MATCH (u) - [r*2] - (u3) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u3.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultsSecond = db.query(querySecond, returns = int)
				for r in resultsSecond:
					if int(r[0]) > 0:
						secondFriendFlag = True
					else:
						secondFriendFlag = False

				#Check third degree friends
				# Query returns 1 if there's a match
				queryThird = 'MATCH (u) - [r*3] - (u4) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u4.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultThird = db.query(queryThird, returns = int)
				for r in resultThird:
					if int(r[0]) > 0:
						thirdFriendFlag = True
					else:
						thirdFriendFlag = False


				#Check fourth degree friends
				# Query returns 1 if there's a match
				queryFourth = 'MATCH (u) - [r*4] - (u5) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u5.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultsFourth = db.query(queryFourth, returns = int)
				for r in resultsFourth:
					if int(r[0]) > 0:
						fourthFriendFlag = True
					else:
						fourthFriendFlag = False

				# If any degree of friends till 4 is true then it's a trusted transaction
				if firstFriendFlag or secondFriendFlag or thirdFriendFlag or fourthFriendFlag:
					data = "trusted"
					output.write(data)
					output.write("\n")

				# IF outside 4th degree friend then it's a unverified transaction
				else:
					data = "unverified"
					output.write(data)
					output.write("\n")

			#If any or both user don't exists in the system then it's an unverified transaction			
			else:
				data = "unverified"
				output.write(data)
				output.write("\n")
	#Close file
	output.close()



# Users outside 2nd degree friends are warned
def featureTwo(args):
	# Boolean flags to identify degree of friend
	firstFriendFlag = False
	secondFriendFlag = False

	#Open output file
	with open(args,'w+') as output:

		# Iterate Stream Payment dataframe
		for index, row in dfPayment.iterrows():

			#Find user nodes in graph based on their id
			find_user1 = graph.find_one("user","num",int(row['id1'].strip()))
			find_user2 = graph.find_one("user","num",int(row['id2'].strip()))

			#If both user exists
			if find_user1 and find_user2:
				
				# Check if immediate friends
				# Query returns 1 if there's a match
				queryFirst = 'MATCH (u) - [r] - (u2) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u2.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultFirst = db.query(queryFirst, returns = int)

				# Update flag based on result
				for r in resultFirst:
					if int(r[0]) > 0:
						firstFriendFlag = True
					else:
						firstFriendFlag = False

				# Check second degree friends
				# Query returns 1 if there's a match
				querySecond = 'MATCH (u) - [r] - (u2)-[r1]-(u3) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u3.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultsSecond = db.query(querySecond, returns = int)

				# Update flag based on result
				for r in resultsSecond:
					if int(r[0]) > 0:
						secondFriendFlag = True
					else:
						secondFriendFlag = False

				# If First or Second Degree Friend flag is true then it's a trusted transaction
				if firstFriendFlag or secondFriendFlag:
					data = "trusted"
					output.write(data)
					output.write("\n")
				
				# If outside 2nd degree friends then it's an unverified transaction		
				else:
					data = "unverified"
					output.write(data)
					output.write("\n")
			#If any or both user don't exists in the system then it's an unverified transaction	
			else:
				data = "unverified"
				output.write(data)
				output.write("\n")

			

	output.close()



def featureOne(stream_payment,output1):

	# Global dataframe to access from other functions
	global dfPayment

	# Create dataframe with two columns
	dfPayment = pd.DataFrame(columns=('id1','id2'))
	
	# Boolean flag to identify immediate or first degree friends
	firstFriendFlag = False
	
	with open(stream_payment) as f:
		csv_f = csv.reader(f)
		# Iteate file and store in dataframe
		for row in csv_f:
			if len(row)>2:
				if "id" not in row[1].strip():
					dfPayment.loc[len(dfPayment)] = [row[1].strip(),row[2].strip()]

	
	# Open output file
	with open(output1,'w+') as output:
		
		# Iterate dataframe
		for index, row in dfPayment.iterrows():

			#Find user nodes in graph based on their id
			find_user1 = graph.find_one("user","num",int(row['id1']))
			find_user2 = graph.find_one("user","num",int(row['id2']))

			#If both user exists
			if find_user1 and find_user2:

				# Check if immediate friends
				# Update flag based on result
				queryFirst = 'MATCH (u) - [r] - (u2) WHERE u.num=toInt('+ str(find_user1['num']) + ') AND u2.num=toInt(' + str(find_user2['num']) + ') RETURN COUNT(u.num)'
				resultFirst = db.query(queryFirst, returns = int)

				# Update flag based on result
				for r in resultFirst:
					if int(r[0]) > 0:
						firstFriendFlag = True
					else:
						firstFriendFlag = False

				# If first degree friend then it's a trusted transaction
				if firstFriendFlag:
					data = "trusted"
					output.write(data)
					output.write("\n")

				#Outside first degree friends, so unverified transaction
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
	
	# Close output file
	output.close()

#Create indices for faster access
def create_indices():
	global graph
	graph.run('CREATE INDEX ON :user(num)')

#Clear any pre-existing graphs
def clear_graph():
	global graph
	clear_cypher = 'MATCH (n) DETACH DELETE n'
	graph.run(statement=clear_cypher)



def create_nodes(args):

	
	# Create dataframe with columns
	df = pd.DataFrame(columns=('id1','id2'))
	#Store file in DataFrame using pandas
	#Read batch_payment.csv file
	with open(args) as f:
		csv_f = csv.reader(f)
		for row in csv_f:
			if len(row)>2:
				if "id" not in row[1].strip():
					df.loc[len(df)] = [row[1].strip(),row[2].strip()]

			

	#Iterate Dataframe
	for index,row in df.iterrows():
		#Find user nodes in graph based on their id
		find_user1 = graph.find_one("user","num",int(row['id1'].strip()))
		find_user2 = graph.find_one("user","num",int(row['id2'].strip()))


		# If user 1 exists
		if find_user1:
			
			# If user 1 and user 2 both exists
			if find_user2:
				
				#Create relationship between both
				rel = Relationship(find_user1,"transaction_with",find_user2,relationship_type="transaction_with")
				graph.create(rel)

			# If user 2 does not exists but user 1 exists
			else:
				
				# Create new user 2 node
				# Create relationship between both
				new_user2 = Node("user", num=int(row['id2'].strip()),node_type="user")
				graph.create(new_user2)
				rel = Relationship(find_user1,"transaction_with",new_user2,relationship_type="transaction_with")
				graph.create(rel)

		#If user 2 exists
		elif find_user2:
			
			# If user 1 does not exits but user 2 exists
			if not find_user1:
					
					# Create new user 1 node
					# Create relationship between both
					new_user1 = Node("user", num=int(row['id1'].strip()),node_type="user")
					graph.create(new_user1)
					rel = Relationship(new_user1,"transaction_with",find_user2,relationship_type="transaction_with")
					graph.create(rel)

		# If both users do not exists
		else:
	
			# Create new user 1 node
			new_user1 = Node("user",num=int(row['id1'].strip()),node_type="user")
			graph.create(new_user1)

			# Create new user 2 node
			new_user2 = Node("user",num=int(row['id2'].strip()),node_type="user")
			graph.create(new_user2)

			# Create relationship between both
			rel = Relationship(new_user1,"transaction_with",new_user2,relationship_type="transaction_with")
			graph.create(rel)
			
		
	
	
 
		


if __name__ == "__main__":
	# Check if command arguments are less than 3
	if len(sys.argv) <=3:
		print("Not valid arguments")
		sys.exit(0)

	try:
		opts, args = getopt.getopt(sys.argv,"hi:o:",["batch_file=","stream_file="])
		batch_payment = args[1]
		stream_payment = args[2]
		output1 = args[3]
		output2 = args[4]
		output3 = args[5]
	except getopt.GetoptError:
   		sys.exit(2)

   	# Clears any pre-existing graphs
	clear_graph()

	# Create user nodes from "batch_payment.csv"
	create_nodes(batch_payment)

	# Implements feature one - First degree friends
	featureOne(stream_payment,output1)

	# Implements feature two - Two degree friends
	featureTwo(output2)

	# Implements feature three - Four degree friends
	featureThree(output3)

