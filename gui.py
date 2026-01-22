import tkinter as tk
from Project import run

def run_app():
    mem = int(mem_entry.get())
    alg = alg_var.get()
    csv_file = "input"

    time_exec = run(mem, csv_file, alg)
    result_label.config(text=f"Execution time: {time_exec:.6f} seconds")

root = tk.Tk()
root.title("Memory Management Fit")

tk.Label(root, text="Memory Size").pack()
mem_entry = tk.Entry(root)
mem_entry.insert(0, "100")
mem_entry.pack()

alg_var = tk.StringVar(value="first")
tk.Radiobutton(root, text="First Fit", variable=alg_var, value="first").pack()
tk.Radiobutton(root, text="Best Fit", variable=alg_var, value="best").pack()
tk.Radiobutton(root, text="Worst Fit", variable=alg_var, value="worst").pack()

tk.Button(root, text="Run", command=run_app).pack()

result_label = tk.Label(root, text="")
result_label.pack()

root.mainloop()
