#include <vector>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <iostream>
#include <pthread.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unordered_map>
#include <queue>
using namespace std;

const int num_threads = 10;
queue<int> clients;
unordered_map<string, string> KV_DATASTORE;
pthread_mutex_t map_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;

void* serverRoutine(void* arg) 
{
    int port = *((int*)arg);
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0)
     {
        cerr << "Error: Couldn't open socket" << endl;
        return NULL;
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
        return NULL;
    }
    if (listen(server_socket, 5) < 0)
     {
        cerr << "Error: Couldn't listen on socket" << endl;
        close(server_socket);
        return NULL;
    }
    cout << "Server listening on port: " << port << endl;
    sockaddr_in client_addr;
    socklen_t caddr_len = sizeof(client_addr);
    while (true)
    {
        int client_socket = accept(server_socket, (sockaddr *)(&client_addr), &caddr_len);
        if (client_socket < 0) {
            cerr << "Error: Couldn't accept connection" << endl;
            continue;
        }
        pthread_mutex_lock(&queue_lock);
        clients.push(client_socket);
        pthread_mutex_unlock(&queue_lock);
    }
    close(server_socket);
    return NULL;
}

int main(int argc, char **argv)
 {
    if (argc != 2)
     {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }
    int port = atoi(argv[1]);
    pthread_t server_thread;
    pthread_create(&server_thread, NULL, &serverRoutine, (void*)&port);
    vector<pthread_t> thread_ids(num_threads);
    for (int i = 0; i < num_threads; i++) 
    {
        pthread_create(&thread_ids[i], NULL, [](void* arg) -> void*
         {
            int server_socket = *((int*)arg);
            while (true)
             {
                int client_socket;
                pthread_mutex_lock(&queue_lock);
                if (!clients.empty())
                 {
                    client_socket = clients.front();
                    clients.pop();
                    pthread_mutex_unlock(&queue_lock);
                    char input[1024];
                    string output="";
                    string dickey="";
                    string value="";
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
                                if(instruction=="END")
                                 {
                                    break;
                                }
                                pthread_mutex_lock(&map_lock);
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
                                    output+="\n";
                                }
                                 else if (instruction == "COUNT")
                                 {
                                    int n=KV_DATASTORE.size();
                                    output = to_string(n) + "\n";
                                }
                                 else if (instruction == "DELETE")
                                  {
                                    getline(stream1, dickey);
                                    if (KV_DATASTORE.find(dickey) != KV_DATASTORE.end()) {
                                        KV_DATASTORE.erase(dickey);
                                        output = "FIN";
                                    } else {
                                        output = "NULL";
                                    }
                                    output+="\n";
                                }
                                pthread_mutex_unlock(&map_lock);
                                send(client_socket, output.c_str(), output.length(), 0);
                                output="";
                                dickey="";
                                value="";
                            }
                        }
                        else
                         {
                            break;
                        }
                    }
                    int res = close(client_socket);
                } 
                else 
                {
                    pthread_mutex_unlock(&queue_lock);
                }
            }
            pthread_exit(NULL);
        }, (void*)&port);
    }
    pthread_join(server_thread, NULL);
    for (int i = 0; i < num_threads; i++) {
        pthread_join(thread_ids[i], NULL);
    }
    return 0;
}
