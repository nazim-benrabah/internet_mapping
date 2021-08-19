import pandas as pd
from neo4j import GraphDatabase
import csv
import time

start_time=time.time()

def csv2neo(dataframe,ID,password):

	driver=GraphDatabase.driver(uri="bolt://localhost:7687",auth=(ID,password))
	session=driver.session()

	
	dataframe = dataframe.applymap(str)

	dests=dataframe["dest_ip"].unique()

	srcs=dataframe["src_ip"].unique()


	dataframe.ttl = pd.to_numeric(dataframe.ttl, errors='coerce')


	for s in srcs:


		for d in dests:

			### Trouver les IP reply correspondant à chaque destination et les trier d'une maniere croissante
			data=dataframe.loc[dataframe['dest_ip']==d].sort_values('ttl',ascending=True)  

			rep=list(data['reply_ip'])

			### relier la source avec le IP reply qui a un ttl le plus petit
			q1="""
			MERGE(A:SOURCE{NAME:$source_name})
			MERGE(B:REPLY{NAME:$reply_name})
			MERGE (A)<-[:GOES_TO]->(B)
			"""

			
			p={"source_name":s,"reply_name":rep[0]}
			session.run(q1,p)

			### relier la destination avec le IP reply qui un ttl le plus grand
			q2="""
			MERGE(B:REPLY{NAME:$reply_name})
			MERGE(D:DESTINATION{NAME:$destination_name})
			MERGE (B)<-[:GOES_TO]->(D)
			"""

			
			p={"reply_name":rep[-1],"destination_name":d}
			session.run(q2,p)

			q3="""
			MERGE(B:REPLY{NAME:$reply_name})
			MERGE(C:REPLY{NAME:$reply_namee})
			MERGE (B)<-[:GOES_TO]->(C)
			"""

			### relier les IP reply intermediaires

			for i in range(len(rep)-1):
				p={"reply_name":rep[i],"reply_namee":rep[i+1]}
				session.run(q3,p)
	
	## query pour retourner les noeuds IP sources du graphe
	q3="""

	MATCH (n:SOURCE) RETURN COUNT(n)

	"""

	## query pour retourner le nombre de noeuds IP REPLY du graphe
	q4="""

	MATCH (n:REPLY) RETURN COUNT(n)

	"""

	## query pour retourner le nombre de noeuds destinations du graphe
	q5="""

	MATCH (n:DESTINATION) RETURN COUNT(n)

	"""

	## query qui retourne le nombre de liens du graphes
	q6="""

	MATCH (n)-[r]->() RETURN COUNT(r)

	"""
	

	## query qui retourne le nombre de noeuds au total
	q7="""

	MATCH (n) RETURN COUNT(n)

	"""

	## execution des queries

	results_nodes=session.run(q7)
	results_sources=session.run(q3)
	results_gateways=session.run(q4)
	results_destinations=session.run(q5)
	results_links=session.run(q6)

	## Résultat des queries
	data_nodes=results_nodes.data()
	data_sources=results_sources.data()
	data_gateways=results_gateways.data()
	data_destinations=results_destinations.data()
	data_links=results_links.data()

	## Affichage des résultats
	print("Nombre total de noeuds = --- %d ---\n" % data_nodes[0]['COUNT(n)'])
	print("Nombre de IP source = --- %d ---\n" % data_sources[0]['COUNT(n)'])
	print("Nombre de IP reply = --- %d ---\n" % data_gateways[0]['COUNT(n)'])
	print("Nombre de IP destination = --- %d --- \n" % data_destinations[0]['COUNT(n)'])
	print("Nombre de liens  = --- %d --- \n" % int(data_links[0]['COUNT(r)']))




filename=str(input('Entrer le nom du fichier .csv : '))

header=['src_ip','dst_prefix','dest_ip','reply_ip','proto','src_port','dst_port', 'ttl','ttl_from_udp_length', 'type', 'code', 'rtt', 'reply_ttl', 'reply_size', 'round', 'snapshot']
ID="neo4j"
password="test"
files=[filename]

for f in files:
	print('------------ Nom du fichier " %s " ------------ \n'%f)
	data=pd.read_csv(f,names=header)
	df=pd.DataFrame(data)

	## comparer les graphes selon les snapshot

	snapshots=df["snapshot"].unique() ### recuperer les numéros de snapshots

	## subdiviser le csv en dataframes en discriminant selon le num de snapshot
	for s in snapshots:
		dataframe=df.loc[df['snapshot']==s]
		print('Snapshot N° %d :'%s)
		csv2neo(dataframe,ID,password)
		print('---------------------------------------------------------------------------')


print("Temps d'execution : ------- %s  secondes ------ " %str(time.time()-start_time))
print('Aller dans http://localhost:7474/browser/ pour visualiser le graphe')