from multiprocessing import Pool

def f(x):
    return x * x

if __name__ == "__main__":
    with Pool(4) as p:
        result = p.map(f, [1, 2, 3, 4])
    print(result)
