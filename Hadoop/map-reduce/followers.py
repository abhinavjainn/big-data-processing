'''
Using a dataset defining a social network as adjacency lists, produce the list of followers of each user.

Example of the dataset:
A B,D
B A,C,D,E
C A,D,E
D C,E
E A,D

Here A follows both B and D; B follows A, C, D and E; etc.
'''


# This program implements inverted index pattern.

# Map step: Receive follower node and list of followed nodes and emit followed node and follower
def mapper(self,follower,line_followed):      # Input - key: follower, value: list of nodes followed by key
	list_followed = line_followed.split(“,”)  # Save the followed nodes in a list by splitting at “,”
	for followed in list_followed:
	yield (followed, follower)			      # Output: Followed node, follower node


# Reduce step: Collect all followers and output against the followed node
def reducer(self,followed,followers): 		# input - key: followed node, value: follower node
	yield(followed,followers) 				# output – followed node, followers