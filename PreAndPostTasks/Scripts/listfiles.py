import os

def main():
    simulation_path = os.environ.get('simulation_path', "/simulation")

    print(f"Listing files under: {simulation_path}")
    for root, dirs, files in os.walk(simulation_path):
        for file in files:
            file_path = os.path.join(root, file)
            print(file_path)

if __name__ == "__main__":
    main()