import os

simulation_path = os.environ.get('simulation_path', "/simulation")
output_path = os.environ.get('output_path', "/output") 

database_file_path = os.path.join(simulation_path, "reference.db")
memberships_file_path = os.path.join(output_path, "memberships_data.csv")

try:
    simulation_id = os.environ.get('simulation_id')
    print(simulation_id)
except Exception as e: 
    print(e)
finally:
	print("Done")
