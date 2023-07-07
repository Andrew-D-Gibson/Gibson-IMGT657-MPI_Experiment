import typing
import time
import random
import sys
import csv
from multiprocessing import Process, Queue

# Define the number of workers we'll be running simultaneously
number_of_processes_to_simulate = 4

# Define the inputs
inputs = [random.randint(0,1000) for _ in range(20)]

# Define the csv filename for output
output_filename = 'output.csv'

MPI_ANY_SOURCE = -1


# Recursive function to determine the length of the collatz sequence for a given number
# See https://en.wikipedia.org/wiki/Collatz_conjecture for more info
def collatz_sequence_length(n:int):
    # n=1 has a sequence length of 0 to match the standard formulation
    if n == 1:
        return 0    
    
    # If n isn't 1, return the length of the next number's collatz sequence plus 1
    if n % 2:
        # Odd integer
        return collatz_sequence_length(3*n + 1) + 1
    else:
        # Even integer
        return collatz_sequence_length(n/2) + 1
    

def mpi_application(
        rank:int,
        size:int,
        send_f:typing.Callable[[typing.Any,int],None],
        recv_f:typing.Callable[[int], typing.Any]
    ):

    if rank == 0:
        ## -- Coordinator logic

        # Define a holder for the outputs
        outputs = []

        # Define a count of how many workers are running at any time.
        # This lets us handle the edge case where the number of inputs is less 
        # than the number of workers assigned to the task.
        workers_running = 0


        # Assign all workers an input and add 1 to the workers_running variable, 
        # or send an 'END' command if there aren't enough inputs for each worker
        for i in range(size-1):
            if len(inputs) > 0:
                send_f(inputs[0], dest=i+1)
                inputs.pop(0)

                workers_running += 1
            else:
                send_f('END', dest=i+1)


        # While there are additional inputs send them to whatever worker returns next
        while len(inputs) > 0:
            response = recv_f(MPI_ANY_SOURCE)

            # Grab the worker's rank and its output from our custom defined return
            # (See the worker logic code below)
            worker_rank = int(response.split(':')[0])
            worker_output = response.split(':')[1]

            # Record the worker's output
            outputs.append(worker_output)

            # Send the worker a new input and remove the input from our list
            send_f(inputs[0], dest=worker_rank)
            inputs.pop(0)


        # All inputs have been assigned, so wait for all the running workers to return
        for i in range(workers_running):
            response = recv_f(MPI_ANY_SOURCE)
            
            # Grab the worker's rank and its output from our custom defined return
            # (See the worker logic code below)
            worker_rank = int(response.split(':')[0])
            worker_output = response.split(':')[1]

            # Record the worker's output
            outputs.append(worker_output)

            # Stop the worker from continuing to listen
            send_f('END', dest=worker_rank)


        workers_running = 0 # Unnecessary, but good to remember


        print(outputs)
        

        # Write the outputs to a csv
        fields = ['Number', 'Length of Collatz Sequence']
        rows = [ [output.split(',')[0], output.split(',')[1]] for output in outputs]

        with open(output_filename, 'w', newline = '') as file:
            csv.writer(file).writerow(fields)
            csv.writer(file).writerows(rows)


    else:
        ## -- Worker logic

        # Listen for instructions from the coordinator
        while True:
            d = recv_f(MPI_ANY_SOURCE)

            # For debugging, display the message received
            print(f"{d} received by {rank}")

            # If the coordinator sent an 'END' message, terminate the worker
            if d == 'END':
                break
            
            # This sleep adds some reasonable time variance between the workers
            # to simulate a more realistic environment
            time.sleep(random.uniform(0,1))

            # Return the workers output in the format required for parsing
            send_f(f"{rank}:{d},{collatz_sequence_length(d)}", dest=0)



###############################################################################
# This is the simulator code, do not adjust

def _run_app(process_rank, size, app_f, send_queues):
    send_f = _generate_send_f(process_rank, send_queues)
    recv_f = _generate_recv_f(process_rank, send_queues)
    
    app_f(process_rank, size, send_f, recv_f)

def _generate_recv_f(process_rank, send_queues):

    def recv_f(from_source:int):
        while send_queues[process_rank].empty():
            time.sleep(1)
        return send_queues[process_rank].get()[1]
    return recv_f


def _generate_send_f(process_rank, send_queues):

    def send_F(data, dest):
        send_queues[dest].put((process_rank,data))
    return send_F


def _simulate_mpi(n:int, app_f):
    
    send_queues = {}

    for process_rank in range(n):
        send_queues[process_rank] = Queue()
    
    ps = []
    for process_rank in range(n):
        
        p = Process(
            target=_run_app,
            args=(
                process_rank,
                n,
                app_f,
                send_queues
            )
        )
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
###############################################################################


if __name__ == "__main__":
    # The collatz sequence occasionally exceeds python's default recursion limit
    # of 1000, so we increase the limit
    sys.setrecursionlimit(100_000_000)

    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
