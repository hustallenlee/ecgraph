# Ecgraph
This is a distributed graph processing engine based on mpi, and it needs further modification so do not clone it although it can run. And Makefile will be added in the future

## Dependency 
(This program depends on below, and it works well in other versions), and also you should make sure that the Makefile locates the right path, in order to share the same libs between the mpi cluster, you can put the libs in a nfs path.
* mpich 3.2
* boost 1.62.0 (for its json parser)
* g++ OR MSVC supporting c++11 (stupid virsual studio do not support MPI Cluster Debugger except virsual studio 2010 [why](https://visualstudio.uservoice.com/forums/121579-visual-studio-ide/suggestions/3075084-bring-back-the-mpi-cluster-debugger))


## Compile
Go into the source path, in a linux system, you should go into the path which includes the Makefile

`cd path/to/source/`

Run the make command

`make`

Then the execute file will be generated in the bin/ path


## Before run
Before you run the program, you should first convert the text graph data to binary graph use our tool, 
## Run

`mpiexec -f machinefile ./bin/algorithm graph_data iterations`
> algorithm is a graph algorithm compiled by mpic++
> graph_data is edge list file, it should be a binary file
> iterations is the number of iterations

