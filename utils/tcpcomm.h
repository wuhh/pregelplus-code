/*
 * Communication.h
 *
 *  Created on: Jun 19, 2014
 *      Author: ylu
 */
#include <netinet/in.h>

#include <sys/types.h>
#include <ifaddrs.h>
#include <arpa/inet.h>

#include <cstring>
#include <vector>

bool str_to_ip(const char* c, uint32_t& out)
{
    if (c == NULL)
        return false;
    else
        return inet_pton(AF_INET, c, &out) > 0;
    /* str to uint32 ip */
}

bool ip_to_str(uint32_t ip, std::string& out)
{
    char ipstring[INET_ADDRSTRLEN] = { 0 };
    const char* ret = inet_ntop(AF_INET, &ip, ipstring, INET_ADDRSTRLEN);
    if (ret == NULL)
        return false;
    out = std::string(ipstring);
    return true;
}

uint32_t get_local_ip(bool print)
{

    uint32_t subnet_id = 0;
    uint32_t subnet_mask = 0;
    std::string str_subnet_id, str_subnet_mask;

    subnet_mask = subnet_id;
    subnet_mask = ntohl(subnet_mask);
    subnet_mask = subnet_mask | (subnet_mask << 1);
    subnet_mask = subnet_mask | (subnet_mask << 2);
    subnet_mask = subnet_mask | (subnet_mask << 4);
    subnet_mask = subnet_mask | (subnet_mask << 8);
    subnet_mask = subnet_mask | (subnet_mask << 16);
    subnet_mask = htonl(subnet_mask);

    ip_to_str(subnet_id, str_subnet_id);
    ip_to_str(subnet_mask, str_subnet_mask);

    // make sure this is a valid subnet address.
    if (print) {
        std::cout << "Subnet ID: " << str_subnet_id << "\n";
        std::cout << "Subnet Mask: " << str_subnet_mask << "\n";
        std::cout
            << "Will find first IPv4 non-loopback address matching the subnet"
            << std::endl;
    }
    uint32_t ip(0);
    // code adapted from
    struct ifaddrs* ifAddrStruct = NULL;
    getifaddrs(&ifAddrStruct);
    struct ifaddrs* firstifaddr = ifAddrStruct;
    if (ifAddrStruct == NULL) {
        std::cout << "ifAddrStruct sould not be null" << std::endl;
        exit(1);
    }
    bool success = false;
    while (ifAddrStruct != NULL) {
        if (ifAddrStruct->ifa_addr != NULL
            && ifAddrStruct->ifa_addr->sa_family == AF_INET) {
            char* tmpAddrPtr = NULL;
            // check it is IP4 and not lo0.
            tmpAddrPtr = (char*)&((struct sockaddr_in*)ifAddrStruct->ifa_addr)->sin_addr;
            if (tmpAddrPtr == NULL) {
                std::cout << "tmpAddrPtr sould not be null" << std::endl;
                exit(1);
            }
            if (tmpAddrPtr[0] != 127) {
                memcpy(&ip, tmpAddrPtr, 4);
                // test if it matches the subnet
                if ((ip & subnet_mask) == subnet_id) {
                    success = true;
                    break;
                }
            }
            //break;
        }
        ifAddrStruct = ifAddrStruct->ifa_next;
    }
    freeifaddrs(firstifaddr);
    if (!success) {
        std::cout << "Unable to find any valid IPv4 address. Defaulting to loopback\n";
        exit(1);
    }
    return ip;
}
std::string get_local_ip_as_str(bool print)
{
    uint32_t ip = get_local_ip(print);
    if (ip == 0)
        return "127.0.0.1";
    else {
        std::string out;
        bool ip_conversion_success = ip_to_str(ip, out);
        if (!ip_conversion_success) {
            std::cout << "IP conversion failed " << std::endl;
        }
        return out;
    }
}
void get_free_tcp_port(size_t& freeport, int& sock)
{
    sock = socket(AF_INET, SOCK_STREAM, 0);
    // uninteresting boiler plate. Set the port number and socket type
    sockaddr_in my_addr;
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = 0; // port 0.
    my_addr.sin_addr.s_addr = INADDR_ANY;
    memset(&(my_addr.sin_zero), '\0', 8);
    if (bind(sock, (sockaddr*)&my_addr, sizeof(my_addr)) < 0) {
        std::cout
            << "Failed to bind to a port 0! Unable to acquire a free TCP port!"
            << std::endl;
    }
    // get the sock information
    socklen_t slen;
    sockaddr addr;
    slen = sizeof(sockaddr);
    if (getsockname(sock, &addr, &slen) < 0) {
        std::cout << "Failed to get port information about bound socket"
                  << std::endl;
    }
    freeport = ntohs(((sockaddr_in*)(&addr))->sin_port);
}
