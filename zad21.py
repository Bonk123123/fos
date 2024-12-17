from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

data = 42 if rank == 0 else None
data = comm.bcast(data, root=0)

print(f"Process {rank} received: {data}")
