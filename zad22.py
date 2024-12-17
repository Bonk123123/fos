from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

value = rank + 1
sum_val = comm.reduce(value, op=MPI.SUM, root=0)

if rank == 0:
    print("Total sum:", sum_val)
