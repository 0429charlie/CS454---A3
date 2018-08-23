#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdlib.h>
#include <iostream>
#include <arpa/inet.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <utility>

#include <assert.h>
#include <map>
#include <strings.h>
#include <vector>
#include "rpc.h"
#include <thread>
#include <limits.h>
#include "LinkedListNode.h"

#include <pthread.h>

#define DEBUG_ON 0

using namespace std;

// Data structure to hold the mapping fo function name and argTypes -> function pointer (skeleton)
map<string, skeleton> functionmap;

// Debugging purposes
void error(const char* msg) {
	perror(msg);
	exit(1);
}

// Some global variables set in rpcInit
int server_sock_fd = 0;
int binder_sock_fd = 0;
struct sockaddr_in serv_addr;

LinkedList *conns_list = new LinkedList();

// Global variables to verify that rpc functions called in correct order
int rpcRegisterCalled = 0;
int rpcInitCalled = 0;

// Forward declarations
void process_request(int sock_fd);
int add_new_connection();
char* get_argTypes_msg(int* argTypes);
void setup_fd_sets(fd_set *read_fds);
void compute_bufferCh(void* arg, int type, char* bufferCh, int &total_bytes, int length);

/* Execution Context: Server
 * Purpose: To set up a connection for clients to connect to and then connect to the binder.
 * Contract: VOID -> INT
 * Return values: 0 if successful initialization
 *               -1 if client connection cannot be established
 *               -2 if binder connection cannot be established
 */
int rpcInit() {
	// 1. Open connection for clients to connect to
	server_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_sock_fd < 0) {
		return -1;
	}

	bzero((char*) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = 0;

	if (::bind(server_sock_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
		return -1;
	}
	listen(server_sock_fd, 100); // Assume 100 for now

	// 2. Open connection to binder
	struct sockaddr_in binder_addr;
	binder_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (binder_sock_fd < 0) {
		return -2;
	}

	char *host = getenv("BINDER_ADDRESS");
	struct hostent *binder = gethostbyname(host);
	if (binder == 0) {
		return -2;
	}
	bzero((char*) &binder_addr, sizeof(binder_addr));
	binder_addr.sin_family = AF_INET;
	bcopy((char *)binder->h_addr, (char *) &binder_addr.sin_addr.s_addr, binder->h_length);
	binder_addr.sin_port = htons(atoi(getenv("BINDER_PORT")));

	if (connect(binder_sock_fd, (const struct sockaddr *) &binder_addr, sizeof(binder_addr)) < 0) {
		return -2;
	}
	rpcInitCalled = 1;
	return 0;
}

/* Execution Context: Server
 * Purpose: To register the server's available functions on the binder.
 * Contract: CHAR* INT* SKELETON -> INT
 * Return values: 0 if successful registration
 *                1 if given function has already been registered
 *               -1 if rpcInit() has not been called
 *               -2 if binder is not located
 *               -3 if binder error - protocol not followed
 */
int rpcRegister(char* name, int* argTypes, skeleton f) {
	if (rpcInitCalled == 0) {
		return -1;
	}
	// Get the server identifier and port number that the server is binding on
	int len = sizeof(serv_addr);
	getsockname(server_sock_fd, (struct sockaddr *)&serv_addr, (socklen_t *) &len);

	char *buffer = new char[INT_MAX];
	char *host_name = new char[INT_MAX];
	char *funcnameargTypes = new char[INT_MAX];
	gethostname(host_name, INT_MAX);
	sprintf(funcnameargTypes, "%s;%s", name, get_argTypes_msg(argTypes));
	sprintf(buffer, "REGISTER;%s;%i;%s;", host_name, ntohs(serv_addr.sin_port), funcnameargTypes);

	// Return value for insert function
	int warning = 0;
	std::pair<std::map<string, skeleton>::iterator,bool> ret;
	ret = functionmap.insert(pair<string, skeleton>(funcnameargTypes, f));
	if (ret.second==false) { // if the function already exist
		warning = 1;
  }
//------------------------------------------------------------------------
	if (DEBUG_ON) {
		cout << "insert: " << funcnameargTypes << endl;
	}
//------------------------------------------------------------------------
	cout << endl;

	// Send request length
	len = strlen(buffer)+1;
	if (write(binder_sock_fd, &len, sizeof(int)) < 0) {
		return -2;
	}

	// Send the register request to the binder
	if (write(binder_sock_fd, buffer, len) < 0) {
		return -2;
	}

	int response = 0;
	if (read(binder_sock_fd, &response, sizeof(int)) < 0) {
		return -2;
	}
	// If there was warning message before
	if (response >= 0 && warning == 1) {
		response = 1;
	}
	// If there is error on the binder side when registering the function
	else if (response == -1) {
		// Revert the funcion to skeleton mapping on server side
		functionmap.erase (funcnameargTypes);
		response = -3;
	}
	rpcRegisterCalled = 1;
	delete[] buffer;
	delete[] funcnameargTypes;
	delete[] host_name;
	return response;
}

/* Execution context: Server
 * Purpose: To receive and service incoming client requests.
 * Contract: VOID -> INT
 * Return Values: 0 if valid terminate request received from binder
 *               -1 if rpcRegister(...) has not yet been called
 *               -2 if select(...) failed
 *               -3 if server cannot connect to client
 */
int rpcExecute() {
	if (rpcRegisterCalled == 0) return -1;
//--------------------------------------------
	if (DEBUG_ON) {
		cout << "EXECUTED" << endl;
	}
//-------------------------------------------
	fd_set read_fds;
	int high_fd = server_sock_fd;

	// 1. listens for connections
	while(1) {
		setup_fd_sets(&read_fds);
		high_fd = max(max(conns_list->getMax(), server_sock_fd), binder_sock_fd);
		if (select(high_fd + 1, &read_fds, 0, 0, 0) < 0) {
			return -2;
		} else {
			if (FD_ISSET(server_sock_fd, &read_fds)) {
				if (add_new_connection() < 0) {
					return -3;
				}
			}
			if (FD_ISSET(binder_sock_fd, &read_fds)) {
//--------------------------------------------------------------------------------
				if (DEBUG_ON) {
					cout << "TERMINATE called" << endl;
				}
//--------------------------------------------------------------------------------
				int response = 0;
				write(binder_sock_fd, &response, sizeof(int));
				delete conns_list;
				break;
			}
			LinkedListNode *current = conns_list->head;
			while (current != 0) {
				LinkedListNode *next = current->next;
				if (FD_ISSET(current->val, &read_fds)) {
					process_request(current->val);
				}
				current = next;
			}
		}
	}
	return 0;
}

/* Execution context: Client
 * Purpose: To execute the given function on a server and return the result.
 * Contract: CHAR* INT* VOID** -> INT
 * Return Values: 0 if given function successfully completed
 *               -1 if client request does not adhere to protocol
 *               -2 if binder connection cannot be established
 *               -3 if given function is registered but no server available
 *               -4 if server connection cannot be established
 *               -5 if given function is not registered, therefore unavailable
 *               -6 if server function execution failed
 */
int rpcCall(char* name, int* argTypes, void** args) {
	// Create client socket to connect to binder-------------------------------------------
	int client_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (client_fd < 0) {
		return -2;
	}
	char *host = getenv("BINDER_ADDRESS");
	struct hostent *binder = gethostbyname(host);
	if (binder == 0) {
		return -2;
	}
	struct sockaddr_in binder_addr;
	bzero((char*) &binder_addr, sizeof(binder_addr));
	binder_addr.sin_family = AF_INET;
	bcopy((char *)binder->h_addr, (char *) &binder_addr.sin_addr.s_addr, binder->h_length);
	binder_addr.sin_port = htons(atoi(getenv("BINDER_PORT")));

	if (connect(client_fd, (const struct sockaddr *) &binder_addr, sizeof(binder_addr)) < 0) {
		return -2;
	}

	// 1. Get location request result from binder-------------------------------------------
	char *argTypes_msg = get_argTypes_msg(argTypes);
	char *buffer = new char[INT_MAX];
	strcat(buffer, "LOC_REQUEST;");
	strcat(buffer, name);
	strcat(buffer, ";");
	strcat(buffer, argTypes_msg);
	strcat(buffer, ";");

	int len = strlen(buffer) + 1;
	if (write(client_fd, &len, sizeof(int)) < 0) {
		return -2;
	}
	if (write(client_fd, buffer, (strlen(buffer) + 1)) < 0) {
		return -2;
	}
	int resp;
	if (read(client_fd, &resp, sizeof(int)) < 0) {
		return -2;
	}
	if (resp < 0) {
		return resp;
	}
	if (read(client_fd, &len, sizeof(int)) < 0) {
		return -2;
	}
	if (read(client_fd, buffer, len) < 0) {
		return -2;
	}

	string response = string(buffer);
	vector<string> tokens;
	size_t pos = 0;
	while((pos = response.find(";")) != string::npos) {
		tokens.push_back(response.substr(0, pos));
		response.erase(0, pos + 1);
	}

	// Create client socket to connect to server------------------------------
	bzero(buffer, INT_MAX - 1);
	close(client_fd);
	client_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (client_fd < 0) {
		return -4;
	}
	struct hostent *server = gethostbyname(tokens.at(0).c_str());
	if (server == 0) {
		return -4;
	}
	struct sockaddr_in serv_addr;
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr, (char *) &serv_addr.sin_addr.s_addr, server->h_length);
	serv_addr.sin_port = htons(stoi(tokens.at(1)));

	if (connect(client_fd, (const struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
		return -4;
	}

	// 2. Send request to server which is handled by rpcExecute()---------------------------------------
	int numArgs = 0;
	while (argTypes[numArgs] != 0) {
		numArgs++;
	}
	char* bufferCh = new char[INT_MAX];
	memcpy(bufferCh, argTypes, (numArgs+1) * sizeof(int));

//-----------------------------------------------------------------
	if (DEBUG_ON) {
		cout << "arg Types" << endl;
		for (int i = 0; i < numArgs+1; i++) {
			cout << argTypes[i] << " ";
		}
	}
//----------------------------------------------------------------

	len = strlen(name) + 1;
	if (write(client_fd, &len, sizeof(int)) < 0) {
		return -4;
	}
	if (write(client_fd, name, len) < 0) {
		return -4;
	}
	if (write(client_fd, &numArgs, sizeof(int)) < 0) {
		return -4;
	}
	if (write(client_fd, bufferCh, (numArgs+1) * sizeof(int)) < 0) {
		return -4;
	}

	for (int i = 0; i < numArgs; i++) {
		int type = (argTypes[i] >> 16) & 15;
		int length = argTypes[i] & 65535;
		int total_bytes = 0;

		if (length == 0) {
			length = 1;
		}

		compute_bufferCh(args[i], type, bufferCh, total_bytes, length);

		if (write(client_fd, &total_bytes, sizeof(int)) < 0 ||
				write(client_fd, bufferCh, total_bytes) < 0) {
					return -4;
		}
	}

	if (read(client_fd, &resp, sizeof(int)) < 0) {
		return -4;
	}
	// Function call unsuccessful
	if (resp < 0) {
		return -6;
	}

	// read response
	for (int i = 0; i < numArgs; i++) {
		int total_bytes = 0;
		// Read server response
		if (read(client_fd, &total_bytes, sizeof(int)) < 0 ||
				read(client_fd, buffer, total_bytes) < 0) {
					return -4;
		}
		if ((argTypes[i] >> ARG_OUTPUT) & 1) {
			args[i] = (void*) malloc(total_bytes);
			memcpy(args[i], buffer, total_bytes);
		}
	}
	delete argTypes_msg;
	delete[] buffer;
	return 0;
}

/* Execution Context: Client
 * Purpose: To send a terminate request to the binder which will terminate all servers
 * Contract: VOID -> INT
 * Return values: 0 if termination success
 *                1 if binder and servers already terminated
 *               -1 if binder connection not established
 */
int rpcTerminate() {
	// Create client socket to connect to binder----------------------------------
	int client_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (client_fd < 0) {
		return -1;
  }
	char *host = getenv("BINDER_ADDRESS");
	struct hostent *binder = gethostbyname(host);
	if (binder == 0) {
		return -1;
	}
	struct sockaddr_in binder_addr;
	bzero((char*) &binder_addr, sizeof(binder_addr));
	binder_addr.sin_family = AF_INET;
	bcopy((char *)binder->h_addr, (char *) &binder_addr.sin_addr.s_addr, binder->h_length);
	binder_addr.sin_port = htons(atoi(getenv("BINDER_PORT")));

	if (connect(client_fd, (const struct sockaddr *) &binder_addr, sizeof(binder_addr)) < 0) {
		return -1;
	}

	char *buffer = new char[INT_MAX];
	strcat(buffer, "TERMINATE;");
	int len = strlen(buffer) + 1;
  if (write(client_fd, &len, sizeof(int)) < 0) {
		return -1;
	}
//---------------------------------------------------------
	if (DEBUG_ON) {
		cout << "Sending Terminate Msg" << endl;
	}
//---------------------------------------------------------
	if (write(client_fd, buffer, len) < 0) {
		return -1;
	}
//---------------------------------------------------------------
	if (DEBUG_ON) {
		cout << "Terminate Msg sent" << endl;
		cout << "Getting response from binder" << endl;
	}
//---------------------------------------------------------------
	int response;
	if (read(client_fd, &response, sizeof(int)) < 0) {
		return - 1;
	}
//----------------------------------------------------------------
	if (DEBUG_ON) {
		cout << "Got the response: " << response << endl;
	}
//----------------------------------------------------------------
	delete [] buffer;
	return response;
}

void compute_bufferCh(void* arg, int type, char* bufferCh, int &total_bytes, int length) {
	switch (type) {
		case ARG_CHAR:
			total_bytes = sizeof(char) * length;
			memcpy(bufferCh, (char*) arg, total_bytes);
			break;
		case ARG_SHORT :
			total_bytes = sizeof(short) * length;
			memcpy(bufferCh, (short*) arg, total_bytes);
			break;
		case ARG_INT:
			total_bytes = sizeof(int) * length;
			memcpy(bufferCh, (int*) arg, total_bytes);
			break;
		case ARG_LONG:
			total_bytes = sizeof(long) * length;
			memcpy(bufferCh, (long*) arg, total_bytes);
			break;
		case ARG_DOUBLE:
			total_bytes = sizeof(double) * length;
			memcpy(bufferCh, (double*) arg, total_bytes);
			break;
		case ARG_FLOAT:
			total_bytes = sizeof(float) * length;
			memcpy(bufferCh, (float*) arg, total_bytes);
			break;
		}
}

char* get_argTypes_msg(int* argTypes) {
	char* result = new char[INT_MAX];
	for (int i = 0; argTypes[i] != 0; i++) {
		int type = (argTypes[i] >> 16) & 15;
		switch (type) {
			case ARG_CHAR:
				strcat(result, "char");
				break;
			case ARG_SHORT :
				strcat(result, "short");
				break;
			case ARG_INT:
				strcat(result, "int");
				break;
			case ARG_LONG:
				strcat(result, "long");
				break;
			case ARG_DOUBLE:
				strcat(result, "double");
				break;
			case ARG_FLOAT:
				strcat(result, "float");
				break;
		}
		int length = argTypes[i] & 65535;
		if (length > 1) {
			strcat(result, "*");
		}
		if (argTypes[i+1] != 0) {
			strcat(result, ",");
		}
	}
	return result;
}

// Not implemented
int rpcCacheCall(char* name, int* argTypes, void** args) {
	return -1;
}

// Initialize read_fds based on connections
void setup_fd_sets(fd_set *read_fds) {
	FD_ZERO(read_fds);
	FD_SET(server_sock_fd, read_fds);
	FD_SET(binder_sock_fd, read_fds);
	LinkedListNode *current = conns_list->head;
	while (current != 0) {
		FD_SET(current->val, read_fds);
		current = current->next;
	}
}

int add_new_connection() {
	struct sockaddr_in cli_addr;
	int len = sizeof(cli_addr);
	int newsock_fd = accept(server_sock_fd, (struct sockaddr *) &cli_addr, (socklen_t *) &len);
	if (newsock_fd < 0) {
		error("ERROR on accepting");
		return -1;
	}
	conns_list->enqueue(newsock_fd);
	return 0;
}

void close_connection(int sock_fd) {
	conns_list->remove(sock_fd);
}

struct thread_struct {
	int sock_fd;
	string name;
	void* argTypes;
	void** args;
	int numArgs;
};

void *executeFunction(void* t_struct) {

//--------------------------------------------------
	if (DEBUG_ON) {
		cout << " new thread " << endl;
	}
//--------------------------------------------------

	void** args = (void**)((thread_struct*)t_struct)->args;
	int* argTypes = (int*)((thread_struct*)t_struct)->argTypes;
	string name = ((thread_struct*)t_struct)->name;
	int sock_fd = ((thread_struct*)t_struct)->sock_fd;
	int numArgs = ((thread_struct*)t_struct)->numArgs;

	// 1. name contains FN_NAME;int,int,char*; format
	skeleton fn = functionmap[string(name)];

	int len = 0;
	int result = fn(argTypes, args);
	if (result < 0) {
		len = -1;
		write(sock_fd, &len, sizeof(int)); // FAILURE
		return 0;
	} else {
		len = 0;
		write(sock_fd, &len, sizeof(int)); // SUCCESS
	}

//---------------------------------------------------------------
	if (DEBUG_ON) {
		cout << "called fn " << name << endl << endl;
	}
//---------------------------------------------------------------

	char *buffer = new char[INT_MAX];
	for (int i = 0; i < numArgs; i++) {
		int type = (argTypes[i] >> 16) & 15;
		int length = argTypes[i] & 65535;
		int total_bytes = 0;
		if (length == 0) {
			length = 1;
		}
		compute_bufferCh(args[i], type, buffer, total_bytes, length);
		write(sock_fd, &total_bytes, sizeof(int));
		write(sock_fd, buffer, total_bytes);
	}
	delete[] buffer;
	delete[] args;
	delete[] argTypes;
	return 0;
}

//void *process_request(void *arg) {
void process_request(int sock_fd) {
	// 1. len, name
	// 2. numArgs
	// 3. args
	// 4. total_bytes type msg

	//int sock_fd = *(int*)arg;
	char *name = new char[INT_MAX];
	int numArgs;
	int *argTypes;
	void** args;

	int len;
	if (read(sock_fd, &len, sizeof(int)) <= 0) {
		close_connection(sock_fd);
		//return NULL;
		return;
	}

	read(sock_fd, name, len);
	read(sock_fd, &numArgs, sizeof(int));
	argTypes = new int[numArgs+1];

	read(sock_fd, argTypes, (numArgs+1) * sizeof(int));
	args = (void **)malloc(numArgs * sizeof(void *));

	assert(argTypes[numArgs] == 0);

//----------------------------------------------------------------------------
	if (DEBUG_ON) {
		cout << "name: " << name << endl;
		cout << "numArgs: " << numArgs << endl;
		cout << "arg types: ";
		for (int i = 0; i < numArgs+1; i++) {
			cout << argTypes[i] << " ";
		}
		cout << endl;
		cout << "argTypes " << get_argTypes_msg(argTypes) << endl;
	}
//---------------------------------------------------------------------------

	for (int i = 0; i < numArgs; i++) {
		int total_bytes = 0;
		read(sock_fd, &total_bytes, sizeof(int));
		char *buffer = new char[INT_MAX];
		read(sock_fd, buffer, total_bytes);
		args[i] = (void *) buffer;
	}

	strcat(name, ";");
	strcat(name, get_argTypes_msg(argTypes));

	struct thread_struct *t_data = (thread_struct*) malloc(sizeof(thread_struct));
	t_data->argTypes = argTypes;
	t_data->args = args;
	t_data->name = string(name);
	t_data->sock_fd = sock_fd;
	t_data->numArgs = numArgs;

	pthread_t tid;
	pthread_create(&tid, 0, executeFunction, (void*) t_data);
	delete[] name;
	return;
}
