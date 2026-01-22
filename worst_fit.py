from Core import run

MEM_SIZE = 100
CSV_FILE = "input_stress.csv"

time = run(MEM_SIZE, CSV_FILE, "worst")
print(f"WORST FIT execution time: {time:.6f} seconds")