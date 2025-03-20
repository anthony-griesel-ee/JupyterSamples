import time
from eecloud.models import *
from eecloud.cloudsdk import CloudSDK

finished_list = ["CompletedSuccess", "Failed", "Cancelled", "CompletedError"]

def get_simulation(pxc: CloudSDK, simulation_id:GuidValue) -> Contracts_Simulation:
    simulation_response = pxc.simulation.list_simulations(simulation_id=simulation_id, print_message=False)
    simulation_data = pxc.simulation.get_final_response(simulation_response)
    if simulation_data.EventData is None or simulation_data.EventData.SimulationRecords is None:
        raise f"Could not find simulation with Id: {simulation_id}"
    return simulation_data.EventData.SimulationRecords[0]
    
def get_executions(pxc: CloudSDK, execution_id:GuidValue) -> list[Contracts_Simulation]:
    execution_response = pxc.simulation.list_simulations(execution_id=execution_id, print_message=False)
    execution_data = pxc.simulation.get_final_response(execution_response)
    if execution_data.EventData is None or execution_data.EventData.SimulationRecords is None:
        raise f"Could not find execution with Id: {execution_id}"
    return execution_data.EventData.SimulationRecords

def wait_simulation_finish(pxc: CloudSDK, simulation_id:GuidValue) -> Contracts_Simulation:
    running = True
    lastStatus = ""
    current: Contracts_Simulation = get_simulation(pxc, simulation_id)
    while running:
        time.sleep(5) # check every 10 seconds for latest simulation status. 
        current = get_simulation(pxc, simulation_id)
        if(current.Status == lastStatus):
            print('.', end="")
        else:
            print()
            print(current.Status, end="")
            lastStatus = current.Status
        
        if current.Status in finished_list:
            running = False
            print(f"Done: {current.Status}")
            return current

def wait_simulation_list_finish(pxc: CloudSDK, simulation_list: list[Contracts_Simulation]) -> None:
        for sim in simulation_list:
            if sim.Status not in finished_list:
                print(f"Waiting for {sim.Id.Value}")
                wait_simulation_finish(pxc, sim.Id.Value)
    
def wait_execution_finish(pxc: CloudSDK, execution_id: GuidValue) -> list[Contracts_Simulation]:
        simulations: list[Contracts_Simulation] = get_executions(pxc, execution_id)
        orig_sim_ids : set[str] = {o.Id.Value for o in simulations}
        wait_simulation_list_finish(pxc, simulation_list=simulations)
        
        #check again to make sure initial simulations didnt start more. 
        simulations: list[Contracts_Simulation] = get_executions(pxc, execution_id)
        new_sim_ids : set[str] = {o.Id.Value for o in simulations}
        
        #if more were returned, find the difference and only continue waiting for those
        unchecked_ids = new_sim_ids - orig_sim_ids        
        unchecked_simulations :list[Contracts_Simulation] = [o for o in simulations if o.Id.Value in unchecked_ids]
        
        if unchecked_simulations is not None and len(unchecked_simulations) > 1:
            wait_simulation_finish(pxc, unchecked_simulations)
        
        return get_executions(pxc, execution_id)
            
def download_solution_data(pxc: CloudSDK, solution_id: GuidValue, output_directory: str) -> None:
    pxc.solution.download_solution(solution_id=solution_id, output_directory=output_directory, solution_type="TaskArtifacts", overwrite=True)
    pxc.solution.download_solution(solution_id=solution_id, output_directory=output_directory, solution_type="AgentLog", overwrite=True)
    print("Solution data downloaded")
    
def show_csv(csv_file_path: str, line_count : int = 30):
    with open(csv_file_path, "r") as table:
        for x in range(line_count):
            line = table.readlines(10)
        print(line)

