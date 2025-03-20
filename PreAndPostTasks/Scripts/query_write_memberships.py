import duckdb
import os

simulation_path = os.environ.get('simulation_path', "/simulation")
output_path = os.environ.get('output_path', "/output") 

database_file_path = os.path.join(simulation_path, "reference.db")
memberships_file_path = os.path.join(output_path, "memberships_data.csv")

try:
	with duckdb.connect() as con:
		con.execute("INSTALL sqlite;")
		con.execute("LOAD sqlite;")

		print(f"Configuring Duck to look at existing sqlite database: {database_file_path}")
		con.execute(f"ATTACH '{database_file_path}' (TYPE SQLITE);")
		con.execute("USE reference;")

		print(f"Reading membership data from {database_file_path}")

		#define query against sqlite (xml input)
		query_text = """select cl1.Name as parent_class, cl2.Name as child_class, col.Name as collection, obj1.Name as parent_object, obj2.Name as child_object, '' as subcollection_name
		from t_membership mem 
		inner join t_object obj1 on obj1.object_id = mem.parent_object_id
		inner join t_object obj2 on obj2.object_id = mem.child_object_id
		inner join t_collection col on col.collection_id = mem.collection_id
		inner join t_class cl1 on cl1.class_id = mem.parent_class_id
		inner join t_class cl2 on cl2.class_id = mem.child_class_id"""

		#write data to csv
		con.execute(f"COPY ({query_text}) TO '{memberships_file_path}' WITH (HEADER, DELIMITER ',');")

		#print data for log
		con.sql(f"{query_text}").show()
		print(f"Membership information written to {memberships_file_path}")
    
except Exception as e: 
    print('Writing data to parquet failed:')
    print(e)
finally:
	print("done")