import csv
import os
import time

def init_mem(n):
    return [{"st": 0, "sz": n, "fr": True, "pid": None}]

def pick(mem, req, alg):
    candidates = [(i, b["sz"]) for i, b in enumerate(mem) if b["fr"] and b["sz"] >= req]
    if not candidates:
        return None
    if alg == "first":
        return candidates[0][0]
    if alg == "best":
        return min(candidates, key=lambda x: x[1])[0]
    if alg == "worst":
        return max(candidates, key=lambda x: x[1])[0]
    return None

def alloc(mem, pid, req, alg):
    i = pick(mem, req, alg)
    if i is None:
        return False
    b = mem[i]
    if b["sz"] == req:
        b["fr"] = False
        b["pid"] = pid
    else:
        new_block = {
            "st": b["st"] + req,
            "sz": b["sz"] - req,
            "fr": True,
            "pid": None
        }
        b["sz"] = req
        b["fr"] = False
        b["pid"] = pid
        mem.insert(i + 1, new_block)
    return True

def free(mem, pid):
    for b in mem:
        if not b["fr"] and b["pid"] == pid:
            b["fr"] = True
            b["pid"] = None
            merge(mem)
            return True
    return False

def merge(mem):
    i = 0
    while i < len(mem) - 1:
        if mem[i]["fr"] and mem[i + 1]["fr"]:
            mem[i]["sz"] += mem[i + 1]["sz"]
            mem.pop(i + 1)
        else:
            i += 1

def mem_state(mem):
    return "".join(f"[{'FREE' if b['fr'] else b['pid']}:{b['sz']}]" for b in mem)

def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with open(path, newline="") as f:
        return list(csv.DictReader(f))

def write_output(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "step",
                "algorithm",
                "action",
                "pid",
                "size",
                "memory_state",
                "free_blocks",
                "free_memory"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

def run(mem_size, csv_path, alg):
    mem = init_mem(mem_size)
    ops = load_csv(csv_path)
    out_rows = []

    start_time = time.perf_counter()

    for step, r in enumerate(ops, start=1):
        action = r["action"]
        pid = r["pid"]
        size = r.get("size")

        if action == "alloc":
            alloc(mem, pid, int(size), alg)
        elif action == "free":
            free(mem, pid)

        free_blocks = sum(1 for b in mem if b["fr"])
        free_mem = sum(b["sz"] for b in mem if b["fr"])

        out_rows.append({
            "step": step,
            "algorithm": alg,
            "action": action,
            "pid": pid,
            "size": size,
            "memory_state": mem_state(mem),
            "free_blocks": free_blocks,
            "free_memory": free_mem
        })

    end_time = time.perf_counter()

    write_output(f"output/{alg}_fit.csv", out_rows)

    return end_time - start_time