# Ecgraph
This is a distributed graph processing engine based on mpi, and it needs further modification so do not clone it although it can run. And Makefile will be added in the future

## Dependency 
(This program depends on below, and it works well in other versions)
* mpich 3.2
* boost 1.62.0 (for its json parser)
* g++ OR MSVC supporting c++11 (stupid virsual studio do not support MPI Cluster Debugger except virsual studio 2010 [why](https://visualstudio.uservoice.com/forums/121579-visual-studio-ide/suggestions/3075084-bring-back-the-mpi-cluster-debugger))

## Run

** mpiexec -f machinefile ./bin/algorithm graph_data iterations

> algorithm is a graph algorithm compiled by mpic++

> graph_data is edge list file, it should be a binary file

> iterations is the number of iterations

