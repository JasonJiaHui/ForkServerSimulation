#!/usr/bin/python 
import random
import sys
import pylab as pl

'''
This fork-join system is mainly about how to handle the association between 
the pre-processor and several servers.Note that no matter the pre-processor 
or the servers, they are all based Fist-In-First-Server mode.
Therefore the are based M/M/1 model.

I concede that I partly used the code of sim_mm1.m when I design the buffer of processor 
and servers.

The challenge of the system I think is how to track every task departured from processor,
and how to merge them at joint.

'''

# Create a processor with some properities
class Processor:
	def __init__(self):
		self.next_arrival_time = None
		self.service_time_next_arrival = None
		self.next_departure_time = float("inf")
		self.arrival_time_next_departure = 0
		self.is_busy = 0
		self.queue_length = 0
		self.buffer_content = []

# Create a server with some properities
class Server:
	def __init__(self, name):
		self.name = name
		self.next_arrival_time = float("inf")
		self.next_departure_time = float("inf")
		self.arrival_time_this_departure = 0
		self.number_subtask = 0
		self.is_busy = 0
		self.queue_length = 0
		self.buffer_content = []


def main():

	x = []
	y = []

	# Create a server_list, including server1-server10.
	server_names = ["server1", "server2", "server3", "server4", "server5",
				 "server6", "server7", "server8", "server9", "server10"]
	server_list = []
	for item in server_names:
		server = Server(item)
		server_list.append(server)


	# Initialise the parameters
	n = int(sys.argv[1])
	Tend = int(sys.argv[2])
	random.seed(int(sys.argv[3]))

	master_clock = 0
	T = 0
	N = 0

	arrival_rate = 0.85
	service_rate = 10 / n


	tm = 10.3846
	k = 2.08

	coeff = (tm ** k) / (n ** (1.65 * k))



	# joint: [[orig_time, number], [orig_time, number], ...]
	joint = []
	processor = Processor()
	processor.next_arrival_time = random.expovariate(arrival_rate) + \
									random.uniform(0.05, 0.25)
	processor.service_time_next_arrival = random.expovariate(service_rate)	

	while(master_clock < Tend):

		'''
		First we should find the min time of all the events,the min time means 
		this event should done next.
		Note that I don't store the next_arrival_time of all the servers
		beccause they are same as next_departure_time of processor.
		'''
		event_time = []
		paralle_event_time = []
		event_time.append(processor.next_arrival_time)
		event_time.append(processor.next_departure_time)

		for i in range(len(server_list)):
			event_time.append(server_list[i].next_departure_time)

		min_time = min(event_time)

		# Refresh the master_clock
		master_clock = min_time

		print "master_clock: ", master_clock

		# Maybe some events have same event_time,means some events may have same departure time
		# Note that I also stored the index of event_time,which will be used to determine which server
		for i in range(len(event_time)):
			if (min_time == event_time[i]):
				paralle_event_time.append([min_time, i])

		for i in range(len(paralle_event_time)):
			# If it is arrival event in processor
			if(paralle_event_time[i][1] == 0):
				if processor.is_busy:
					processor.buffer_content.append([processor.next_arrival_time, processor.service_time_next_arrival])
					processor.queue_length += 1

				else:
					processor.next_departure_time = processor.next_arrival_time + processor.service_time_next_arrival
					processor.arrival_time_next_departure = processor.next_arrival_time
					processor.is_busy = 1

				processor.next_arrival_time = master_clock + random.expovariate(arrival_rate) + random.uniform(0.05, 0.25)
				processor.service_time_next_arrival = random.expovariate(service_rate)


			# If it is departure event from processor
			elif(paralle_event_time[i][1] == 1):
				# Preserve the arrival time of this departure
				arrival_time_of_this_departure_save = processor.arrival_time_next_departure

				if processor.queue_length:
					processor.next_departure_time = master_clock + processor.buffer_content[0][1]
					processor.arrival_time_next_departure = processor.buffer_content[0][0]

					processor.buffer_content.remove(processor.buffer_content[0])
					processor.queue_length -= 1

				else:
					processor.next_departure_time = float("inf")
					processor.is_busy = 0


				sub_request_index = 1
				# Select n servers randomly from server_list
				selected_servers = random.sample(server_list, n)

				# Assign the each subtask to each selected_server
				for server in selected_servers:

					service_time_subtask = (coeff / (1 - random.uniform(0,1))) ** (1 / k)
					# denominator = n ** (1.65 * 2.08) * random.uniform(0,1)
					# numerator = 2.08 * (10.3846 ** 2.08)
					# service_time_subtask = (numerator / denominator) ** (1 / 3.08)

					# while(service_time_subtask < (10.3846 / (n ** 1.65))):
					# 	denominator = n ** (1.65 * 2.08) * random.uniform(0,1)
					# 	numerator = 2.08 * (10.3846 ** 2.08)
					# 	service_time_subtask = (numerator / denominator) ** (1 / 3.08)

					if server.is_busy:
						server.buffer_content.append([arrival_time_of_this_departure_save, service_time_subtask, sub_request_index])
						server.queue_length += 1

					else:
						server.next_departure_time = master_clock + service_time_subtask
						server.arrival_time_next_departure = arrival_time_of_this_departure_save
						server.number_subtask = sub_request_index
						server.is_busy = 1

					sub_request_index += 1

			# Departure event from servers
			# Note that when the departure from servers happened
			# It should check whether it's in joint,if not append([orig_time, 1])
			# else, increment the number of associated orig_time.
			# Then check whether the number == n,if yes,means this is a integrity task.
			# and then T = master_clock - orig_time(this task) and N += 1 and the remove it
			else:
				run_server = server_list[paralle_event_time[i][1] - 2]
				origianl_arrival_time = run_server.arrival_time_next_departure

				# joint = [[orig_time, number], [], ...]

				if n == 1:
					T += master_clock - origianl_arrival_time
					N += 1
					x.append(master_clock)
					y.append(T/N)

				else:

					if(joint == []):
						joint.append([origianl_arrival_time, 1])

					else:
						found_flag = False
						for i in range(len(joint)):
							if origianl_arrival_time in joint[i]:
								found_flag = True

						if found_flag:
							for i in range(len(joint)):
								if joint[i] != [] and joint[i][0] == origianl_arrival_time:
									joint[i][1] += 1

									if joint[i][1] == n:
										T += master_clock - origianl_arrival_time
										N += 1
										x.append(master_clock)
										y.append(T/N)
										joint[i] = []
						else:
							joint.append([origianl_arrival_time, 1])

				# If the there are no tasks in queue,initialise it as Inf
				if (run_server.queue_length == 0):
					run_server.next_departure_time = float("inf")
					run_server.is_busy = 0

				# Else,schedule the next event
				else:
					run_server.next_departure_time = master_clock + run_server.buffer_content[0][1]
					run_server.arrival_time_next_departure = run_server.buffer_content[0][0]
					run_server.number_subtask = run_server.buffer_content[0][2]
					run_server.buffer_content.remove(run_server.buffer_content[0])
					run_server.queue_length = run_server.queue_length - 1



	print "T: ", T
	print "N: ", N
	print "Response: ", T/N
	# pl.xlabel("master_clock")
	# pl.ylabel("Mean response time at master_clock")
	# pl.title("Mean response time at master_clock when n = " + str(n))
	# pl.plot(x,y)
	# pl.show()


if __name__ == '__main__':
	main()
















