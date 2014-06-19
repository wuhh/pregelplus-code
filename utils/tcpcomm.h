/*
 * Communication.h
 *
 *  Created on: Jun 19, 2014
 *      Author: ylu
 */
#include <netinet/in.h>
#include <cstring>
#include <vector>

std::pair<size_t, int> get_free_tcp_port(size_t& freeport,int& sock) {
  sock = socket(AF_INET, SOCK_STREAM, 0);
  // uninteresting boiler plate. Set the port number and socket type
  sockaddr_in my_addr;
  my_addr.sin_family = AF_INET;
  my_addr.sin_port = 0; // port 0.
  my_addr.sin_addr.s_addr = INADDR_ANY;
  memset(&(my_addr.sin_zero), '\0', 8);
  if (bind(sock, (sockaddr*)&my_addr, sizeof(my_addr)) < 0){
	  std::cout << "Failed to bind to a port 0! Unable to acquire a free TCP port!" << std::endl;
  }
  // get the sock information
  socklen_t slen;
  sockaddr addr;
  slen = sizeof(sockaddr);
  if (getsockname(sock, &addr, &slen) < 0) {
    std::cout <<  "Failed to get port information about bound socket" << std::endl;
  }
  freeport = ntohs(((sockaddr_in*)(&addr))->sin_port);
}


