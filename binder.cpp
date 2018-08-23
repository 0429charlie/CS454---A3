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
#include <map>
#include <queue>
#include <list>
#include <set>

#include <limits.h>
#include <algorithm>

using namespace std;

#define DEBUG_ON 0 

// Data structure for registered servers and their function
// This is a dictionary. The Key is the concatenated function name and argument types. The value is a queue of pairs where the first element of the pair is the server_id and the second element is the port number of the server having the function
map < string, set < pair < string, string > > > db;

// Data structure for keeping track of the server (key) and its file descriptor (value)
list < pair < string, int > > activeservers;

int main(int args, char * argv[]) {

  db.clear();

  struct sockaddr_in serv_addr;
  struct sockaddr_in addr;

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    cerr << "Error getting socket file description" << endl;
    exit(1);
  }

  serv_addr.sin_family = AF_INET;;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = 0;

  int status;

  status = ::bind(sockfd, (struct sockaddr * ) & serv_addr, sizeof(serv_addr));
  if (status == -1) {
    cerr << "Error binding" << endl;
    exit(1);
  }

  // Print out the binder address and port that it is binding on
  int addr_len = sizeof(addr);
  getsockname(sockfd, (struct sockaddr * ) & addr, (socklen_t * ) & addr_len);
  char * str = new char[INT_MAX];
  gethostname(str, INT_MAX);
  cout << "BINDER_ADDRESS " << str << endl;
  cout << "BINDER_PORT " << ntohs(addr.sin_port) << endl;

  // Listen to all the socket
  listen(sockfd, 100);
  fd_set master; // master file descriptor list
  fd_set read_fds; // temp file descriptor list for select()
  int fdmax; // maximum file descriptor number
  int newfd; // newly accept()ed socket descriptor
  struct sockaddr_storage remoteaddr; // client or server address
  socklen_t addrlen;
  char buf[8192]; // buffer for client or server data
  int nbytes;

  FD_ZERO( & master);
  FD_ZERO( & read_fds);
  fdmax = sockfd;

  FD_SET(sockfd, & master);

  for (;;) {
    read_fds = master;
    select(fdmax + 1, & read_fds, NULL, NULL, NULL);
    if (select(fdmax + 1, & read_fds, NULL, NULL, NULL) == -1) {
      cerr << "Failed at select" << endl;
      exit(1);
    }

    // run through the existing connections looking for data to read
    for (int i = 0; i <= fdmax; i++) {
      if (FD_ISSET(i, & read_fds)) {
        if (i == sockfd) {
          // handle new connections
          addrlen = sizeof(remoteaddr);
          newfd = accept(sockfd, (struct sockaddr * ) & remoteaddr, & addrlen);

          if (newfd == -1) {
            cerr << "Can't accept new connect request" << endl;
            exit(1);
          } else {
            FD_SET(newfd, & master); // add to master set
            if (newfd > fdmax) { // keep track of the max
              fdmax = newfd;
            }
          }
        } else {
          // handle data from a client
          int len;
          nbytes = read(i, & len, sizeof(int));
          if (nbytes <= 0) {
            // got error or connection closed by client
            if (nbytes == 0) {
              //cerr << "The connection is closed" << endl;
              close(i); // bye!
              FD_CLR(i, & master); // remove from master set
            } else {
              //cerr << "recv error" << endl;
              close(i); // bye!
              FD_CLR(i, & master); // remove from master set
            }
          } else {
            read(i, buf, len);

            string response = string(buf);
            vector < string > tokens;
            size_t pos = 0;
            while ((pos = response.find(";")) != string::npos) {
              tokens.push_back(response.substr(0, pos));
              response.erase(0, pos + 1);
            }

            // Handle the request depends on the type of request
            if (tokens.at(0) == "REGISTER") { // If it is a register request from server
              // Update the server list
              string s_sid = tokens.at(1);
              string s_port = tokens.at(2);
              string s_sidport = s_sid + ";" + s_port + ";";
              string pnameandargTypes = tokens.at(3) + ";" + tokens.at(4);

	      // Message not following the protocol
	      if (tokens.size() != 5) {
                int response = -1;
		write(i, & response, sizeof(int));
              } else {
		
		// Update only if nit recorded
		pair < string, int > p;
		p.first = s_sidport;
		p.second = i;
		if (std::find(activeservers.begin(), activeservers.end(), p) == activeservers.end()) {
              		activeservers.push_back(pair < string, int > (s_sidport, i));
		}

              	// Update the dictionary
              	pair < string, string > newlocation(s_sid, s_port);
              	if (db.find(pnameandargTypes) == db.end()) { // If the functino is never registered
                	set < pair < string, string > > s;
                	s.insert(newlocation);
			db.insert(pair < string, set < pair < string, string > > > (pnameandargTypes, s));
              	} else { // If the function is registered already
                	db[pnameandargTypes].insert(newlocation);
              	}
              	// Send the the register success message
              	int response = 0;
              	write(i, & response, sizeof(int));

	      }

//-----------------------------------------------------------------------------------------------------------------------------------
              if (DEBUG_ON) {
                /*
                cout << endl;
                for (map < string, set < pair < string, string > > > ::const_iterator it = db.begin(); it != db.end(); ++it) {
                  cout << it -> first << " " << endl;
                  // Print first element for simplicity
                  cout << it -> second.front().first << " " << it -> second.front().second << endl;
                }
                cout << endl;
                */
              }
//------------------------------------------------------------------------------------------------------------------------------------

            } else if (tokens.at(0) == "LOC_REQUEST") { // If it is a lacation request
		int response = 0;
		// Message not following the protocol
		if (tokens.size() != 3) {
			response = -1;
			write(i, &response, sizeof(int));
		}

              string pnameandargTypes = tokens.at(1) + ";" + tokens.at(2);

//------------------------------------------------------------------------
	      if (DEBUG_ON) {
              	cout << pnameandargTypes << endl;
	      }
//------------------------------------------------------------------------
              // Look up the database
              if (db.find(pnameandargTypes) == db.end()) { // Can't find the key
                response = -5;
                write(i, & response, sizeof(int));
              }
              // If the key found
              else if (db[pnameandargTypes].empty()) { // The queue is empty
                response = -3;
                write(i, & response, sizeof(int));
              } else {
                for (list<pair<string, int>>::iterator it = activeservers.begin(); it != activeservers.end(); ++it) {

		  if (DEBUG_ON) {
		  	cout << "For: " << it->first << endl;
		  	cout << "The activeservers are: " << endl;
		  	for (list<pair<string, int>>::iterator itt = activeservers.begin(); itt != activeservers.end(); ++itt) {
				cout << itt->first << endl;
		  	}
		  }

                  string sid = it->first;

                  vector < string > tokens;
                  size_t pos = 0;
                  while ((pos = sid.find(";")) != string::npos) {
                    tokens.push_back(sid.substr(0, pos));
                    sid.erase(0, pos + 1);
                  }
                  string server_id = tokens.at(0);
                  string port_num = tokens.at(1);

                  pair<string, string> loc_pair (server_id, port_num);

                  if (db[pnameandargTypes].find(loc_pair) != db[pnameandargTypes].end()) {
		    if (DEBUG_ON) {
		    	cout << "function, " << pnameandargTypes << ", registered with: " << it->first << " is requested" << endl;
		    }
                    char * buffer = new char[INT_MAX];
                    strcat(buffer, server_id.c_str());
                    strcat(buffer, ";");
                    strcat(buffer, port_num.c_str());
                    strcat(buffer, ";");
                    int len = strlen(buffer) + 1;

		    //cout << "update the active server" << endl;
		    activeservers.push_back(*it);
                    activeservers.erase(it);
		    if (DEBUG_ON) {
		    	for (list<pair<string, int>>::iterator itt = activeservers.begin(); itt != activeservers.end(); ++itt) {
                        	cout << itt->first << endl;
                    	}
		    }

                    write(i, & response, sizeof(int));
                    write(i, & len, sizeof(int));
                    write(i, buffer, len);

             	    break;
                  }
                  //break;
                }
              }
            } else if (tokens.at(0) == "TERMINATE") { // If it is a termination request
              // Send terminate message to all active servers
	      int terminate = 0;
	      int rt = 0;
              for (list<pair<string, int>>::iterator it = activeservers.begin(); it != activeservers.end(); ++it) {
		if (rt == 0) {	// No server not available so far
			// rt will be assign to -1 if the server is not available (close already etc.)
			rt = write(it -> second, &terminate, sizeof(int));
			if (rt == -1) {
				// Thus, we want to return warning to client
				rt = 1;
			} else {
				// We still want to read the return message sicne the server is available
				read(it -> second, &terminate, sizeof(int));
				rt = 0;
			}
		} else {	// There were a server not available already. Going to return warning message anyways
			// rt will be assign to -1 if the server is not available (close already etc.)
			rt = write(it -> second, &terminate, sizeof(int));
			if (rt == -1) {	// Another server not avaialble
                                rt = 1;
                        } else {	// This one is available
                                read(it -> second, &terminate, sizeof(int));
				rt = 1;	// But still need to return warning to client
                        }
		}
              }

              // Send the success message back to client
              write(i, &rt, sizeof(int));
              exit(0);
            } else { // If it is invalid request (one does not follow the protocol)
		int ret = -1;
		write(i, &ret, sizeof(int));
            }
          }
        }
      }
    }
  }
}
