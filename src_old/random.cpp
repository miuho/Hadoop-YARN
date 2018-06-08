#include "TetrischedService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "YARNTetrischedService.h"
#include <thrift/transport/TSocket.h>                                                                                             
#include <thrift/transport/TTransportUtils.h>  

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <vector>
#include <queue>
#include <set>
#include <mutex>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

//using boost::shared_ptr;

using namespace std;
using namespace alsched;

struct Job_S {
    JobID jobId;
    job_t::type jobType;
    int32_t k;
    int32_t priority;
    double duration;
    double slowDuration;
};

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{

private:
    vector<bool> available_slaves;
    queue<Job_S *> waiting_jobs;
    mutex mtx;
    
    void TryToAllocate() {
        int yarnport = 9090;
        boost::shared_ptr<TTransport> socket(new TSocket("localhost", yarnport));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        YARNTetrischedServiceClient client(protocol);
        set<int32_t> machines;
        // try to allocate some nodes
        try {
            transport->open();
            
            // keep allocating resources for as many jobs as possible
            while (!waiting_jobs.empty()) {
                machines.clear();
                
                // get the head of the queue
                Job_S *head = waiting_jobs.front();
                cout << "Handling job " << head->jobId << "\n";
                cout << "jobId: " << head->jobId << ", jobType: " << head->jobType << ", k: " << head->k << ", priority: "
                    << head->priority << ", duration: " << head->duration << ", slowDuration: " << head->slowDuration << "\n";
                
                // find highest ranked available slaves
                vector<int32_t> tmp;
                for (int32_t i = 0; i < (int32_t)available_slaves.size(); i++) {
                    if (available_slaves[i])
                        tmp.push_back(i);
                }
                
                // not enough resources to allocate to head
                if ((int32_t)tmp.size() < head->k) {
                    cout << "Not enough resources\n";
                    break;
                }
                    
                // get k highest ranked available slaves
                /*
                set<int32_t> machines;
                cout << "Prepare to allocate slaves: ";
                for (int j = 0; j < head->k; j++) {
                    machines.insert(tmp[j]);
                    available_slaves[tmp[j]] = false;
                    cout << tmp[j]  << ", ";
                }
                cout << "\n";
                */
                
                // get k available slaves randomly
                srand (time(NULL));
                int j = 0;
                cout << "Prepare to allocate slaves: ";
                while (j < head->k) {
                    int rand_index = rand() % tmp.size();
                    machines.insert(tmp[rand_index]);
                    available_slaves[tmp[rand_index]] = false;
                    cout << tmp[rand_index]  << ", ";
                    tmp.erase(tmp.begin() + rand_index);
                    j++;
                }
                cout << "\n";
                
                // allocate machines
                client.AllocResources(head->jobId, machines);
                
                cout << "Successfully handled job " << head->jobId << "\n";
                // free job info
                waiting_jobs.pop();
                delete head;
            }
            
            transport->close();
        } catch (TException& tx) {
            printf("ERROR calling YARN : %s\n", tx.what());
            
            // recover
            printf("Recover: \n");
            for (std::set<int32_t>::iterator it = machines.begin(); it != machines.end(); ++it) {
                available_slaves[*it] = true;
                cout << *it  << ", ";
            }
            cout << "\n";
        }
    }
    
public:

    TetrischedServiceHandler(char *config_file)
    {
        // Your initialization goes here
        printf("random\n");
        int free_machines = 0;
        if (config_file != NULL) {
            ifstream in(config_file);
            if (!in)
                printf("ERROR: cannot find config file\n");
            else {
                string line;
                string topology = "";
                while (getline(in, line)) {
                    if (line.empty())
                        continue;
                    else {
                        size_t found = line.find("rack_cap");
                        if (found != string::npos) {
                            string tmp = line.substr(found);
                            topology.append(tmp);
                            if (tmp.find("]") != string::npos)
                                break;
                            while (getline(in, line)) {
                                if (line.empty())
                                    continue;
                                else {
                                    topology.append(line);
                                    if (line.find("]") != string::npos)
                                        break;
                                }
                            }
                        }
                    }
                    if (topology != "")
                        break;
                }
                in.close();
                if (topology == "" || topology.find("[") == string::npos || topology.find("]") == string::npos ||
                    topology.find("[") > topology.find("]")) {
                    printf("ERROR: invalid config file\n");
                    return;
                }
                topology = topology.substr(topology.find("["), topology.find("]"));
                cout << topology << "\n";
                for (unsigned int i = 0; i < topology.length(); i++) {
                    char c = topology.at(i);
                    if (isdigit(c)) {
                        int num = c - '0';
                        // all slaves in a rack are initially free
                        for (int i = free_machines; i < (free_machines + num); i++)
                            available_slaves.push_back(true);
                        free_machines += num;
                    }
                }
                cout << "Total Slave VM Count: " << free_machines << "\n";
            }
        }
        else {
            printf("ERROR: no input config file\n");
        }
    }
    
    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k, const int32_t priority, const double duration, const double slowDuration)
    {
        mtx.lock();
        printf("AddJob\n");
        
        // create Job_S structure to store job info
        Job_S *job  = new Job_S;
        job->jobId = jobId;
        job->jobType = jobType;
        job->k = k;
        job->priority = priority;
        job->duration = duration;
        job->slowDuration = slowDuration;
        
        // push job to queue
        waiting_jobs.push(job);
        cout << "Pushed job " << jobId << " to queue\n";

        TryToAllocate();

        mtx.unlock();
    }

    void FreeResources(const std::set<int32_t> & machines)
    {
        mtx.lock();
        printf("FreeResources\n");
        
        // free resources
        cout << "Free slaves: ";
        for (std::set<int32_t>::iterator it = machines.begin(); it != machines.end(); ++it) {
            available_slaves[*it] = true;
            cout << *it  << ", ";
        }
        cout << "\n";
        
        TryToAllocate();
        
        mtx.unlock();
    }
};

int main(int argc, char **argv)
{
    //create a listening server socket
    int alschedport = 9091;
    char *config_file = NULL;
    if (argc >= 2)
	config_file = argv[1];
    boost::shared_ptr<TetrischedServiceHandler> handler(new TetrischedServiceHandler(config_file));
    boost::shared_ptr<TProcessor> processor(new TetrischedServiceProcessor(handler));
    boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(alschedport));
    boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}

