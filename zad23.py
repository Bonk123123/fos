from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

data = [1, 2, 3, 4] if rank == 0 else None
recv_val = comm.scatter(data, root=0)
res = recv_val * 10

collected = comm.gather(res, root=0)
if rank == 0:
    print(collected)
