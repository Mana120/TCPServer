#include <iostream>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <pthread.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unordered_map>
using namespace std;
unordered_map<string, string> KV_DATASTORE;
int main(int argc, char **argv) 
{
    int port;
    if (argc != 2)
	{
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }
    port = atoi(argv[1]);
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (server_socket < 0) 
	{
        cerr << "Error: Couldn't open socket" << endl;
        return -1;
    }
    struct sockaddr_in server_addr;
    socklen_t saddr_len = sizeof(server_addr);
    memset(&server_addr, 0, saddr_len);
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    if (bind(server_socket, (struct sockaddr *)&server_addr, saddr_len) < 0) 
	{
        cerr << "Error: Couldn't bind socket" << endl;
        close(server_socket);
        return -1;
    }
    if (listen(server_socket, 5) < 0) {
        cerr << "Error: Couldn't listen on socket" << endl;
        close(server_socket);
        return -1;
    }
    cout << "Server listening on port: " << port << endl;
    sockaddr_in client_addr;
    socklen_t caddr_len = sizeof(client_addr);
    int client_socket;
    while (true) 
	{
        client_socket = accept(server_socket, (sockaddr *)(&client_addr), &caddr_len);
        if (client_socket < 0)
		 {
            cerr << "Error: Couldn't accept connection" << endl;
            exit(1);
        }
        char input[1024];
        string output = "";
        string dickey = "";
        string value = "";
        while (true)
		 {
            memset(input, 0, sizeof(input));
            int rec = recv(client_socket, input, sizeof(input), 0);
            if (rec > 0) 
			{
                string instruction;
                stringstream stream1(input);
                while (getline(stream1, instruction)) 
				{
                    if (instruction == "WRITE") 
					{
                        getline(stream1, dickey);
                        getline(stream1, value);
                        value = value.substr(1);

                        KV_DATASTORE[dickey] = value;
                        output = "FIN\n";
                    }
					else if (instruction == "READ")
					 {
                        getline(stream1, dickey);

                        if (KV_DATASTORE.find(dickey) != KV_DATASTORE.end()) {
                            output = KV_DATASTORE[dickey];
                        } else {
                            output = "NULL";
                        }
                        output += "\n";
                    }
					 else if (instruction == "COUNT")
					  {
                        int n = KV_DATASTORE.size();
                        output = to_string(n) + "\n";
                    }
					 else if (instruction == "DELETE") 
					 {
                        getline(stream1, dickey);
                        if (KV_DATASTORE.find(dickey) != KV_DATASTORE.end()) 
						{
                            KV_DATASTORE.erase(dickey);
                            output = "FIN";
                        }
						 else 
						 {
                            output = "NULL";
                        }
                        output += "\n";
                    } 
					else if (instruction == "END") 
					{
                        break;
                    }
                    send(client_socket, output.c_str(), output.length(), 0);
                    output = "";
                    dickey = "";
                    value = "";
                }
            } 
			else
			{
                close(client_socket);
                break;
            }
        }
    }

    close(server_socket);
    return 0;
}
