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
#include <algorithm>
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
#include <unordered_map>
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
    int free_machines;
    vector<vector<int32_t>> free_racks;
    vector<vector<int32_t>> used_racks;
    unordered_map<int32_t, time_t> free_times;
    //queue<Job_S *> waiting_jobs;
    set<Job_S *> MPI_jobs;
    set<Job_S *> GPU_jobs;
    mutex mtx;

    time_t get_wait_time(time_t cur_time, int32_t k) {
        time_t global_rack_min = cur_time;
        for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
            if (k > (int32_t)(free_racks[i].size() + used_racks[i].size()))
                continue;
            int32_t require = k - free_racks[i].size();
            vector<time_t> times;
            for (int32_t j = 0; j < (int32_t)used_racks[i].size(); j++) {
                time_t t = free_times[used_racks[i][j]];
                time_t diff = (t > cur_time) ? (t - cur_time) : 0;
                times.push_back(diff);
            }
            sort(times.begin(), times.end());
            time_t rack_min = times[require - 1];
            if (rack_min < global_rack_min)
                global_rack_min = rack_min;
        }
        return global_rack_min;
    }
    
    vector<Job_S *> getJobs(set<Job_S *> set_jobs){

        cout << "In getJobs()" << "\n";
        vector<Job_S *> result; 

        // should never go into this 
        if (set_jobs.empty()){
            cout << "Empty jobs\n";
            return result;
        }

        double min_fduration = 999999;
        double min_sduration = 999999;
        Job_S * tmp1;
        Job_S * tmp2;
        for (std::set<Job_S *>::iterator it = set_jobs.begin(); it != set_jobs.end(); ++it) {
            double cur_fduration = (*it)->duration;
            double cur_sduration = (*it)->slowDuration;

            if (cur_fduration < min_fduration){
                tmp1 = *it;
                min_fduration = cur_fduration;
            }
            if (cur_sduration < min_sduration){
                tmp2 = *it;
                min_sduration = cur_sduration;
            }

        }
        result.push_back(tmp1);
        result.push_back(tmp2);
        cout << "Found job with min duration: " << "jobId: " << tmp1->jobId << ", jobType: " << tmp1->jobType << ", k: " << tmp1->k << ", priority: "
                    << tmp1->priority << ", duration: " << tmp1->duration << ", slowDuration: " << tmp1->slowDuration << "\n";

        cout << "Found job with min slow duration: " << "jobId: " << tmp2->jobId << ", jobType: " << tmp2->jobType << ", k: " << tmp2->k << ", priority: "
                    << tmp2->priority << ", duration: " << tmp2->duration << ", slowDuration: " << tmp2->slowDuration << "\n";

        return result;

    }


    
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
            while (!MPI_jobs.empty() || !GPU_jobs.empty()) {
                machines.clear();
                int duration = 0;
                
                Job_S *head(0);
                Job_S *MPI_head(0);
                Job_S *GPU_head(0);
                int caseMPI = -1;
                int caseGPU = -1;

                // get the required job from the queue
                //Job_S *head = waiting_jobs.front();
                if (!MPI_jobs.empty()){
                    vector<Job_S *> tuple_MPI = getJobs(MPI_jobs);
                    Job_S * head_MPI_fduration = tuple_MPI[0];
                    Job_S * head_MPI_sduration = tuple_MPI[1];
                    cout <<":)"<<"head_MPI_fduration_id: "<< head_MPI_fduration->jobId<<"\n";
                    cout <<":)"<<"head_MPI_sduration_id: "<< head_MPI_sduration->jobId<<"\n";

                    //Job_MPI has resource
                    for (int32_t i = 1; i < (int32_t)free_racks.size(); i++) {
                        if ((int32_t)free_racks[i].size() >= head_MPI_fduration->k) {
                            MPI_head = head_MPI_fduration;
                            caseMPI = 1;
                            break;
                        }
                    }
                    cout<<"MPI_head="<<MPI_head<<"\n";
                    if(!MPI_head){
                        MPI_head = head_MPI_sduration;
                        caseMPI = 0;
                    }

                }
                if (!GPU_jobs.empty()){
                    vector<Job_S *> tuple_GPU = getJobs(GPU_jobs);
                    Job_S * head_GPU_fduration = tuple_GPU[0];
                    Job_S * head_GPU_sduration = tuple_GPU[1];

                    //Job_GPU has resource
                    if ((int32_t)free_racks[0].size() >= head_GPU_fduration->k){
                        GPU_head = head_GPU_fduration;
                        caseGPU = 1;
                    } else{
                        GPU_head = head_GPU_sduration;
                        caseGPU = 0;
                    }

                }
                cout <<"caseMPI = " << caseMPI << "\n";
                cout <<"caseGPU = " << caseGPU << "\n";

                if (caseMPI == -1 && caseGPU == -1){
                    cout<<"Invalid case number combination\n";
                    break;
                } 
                else if(caseMPI == -1){
                    head = GPU_head;
                }
                else if(caseGPU == -1){
                    head = MPI_head;
                }
                else if(caseMPI == 0 && caseGPU == 0){
                    if ((MPI_head->slowDuration) < (GPU_head->slowDuration)){
                        head = MPI_head;
                    } else{
                        head = GPU_head;
                    }
                }
                else if(caseMPI == 1 && caseGPU == 0){
                    if ((MPI_head->duration) < (GPU_head->slowDuration)){
                        head = MPI_head;
                    } else{
                        head = GPU_head;
                    }
                }
                else if(caseMPI == 0 && caseGPU == 1){
                    if ((MPI_head->slowDuration) < (GPU_head->duration)){
                        head = MPI_head;
                    } else{
                        head = GPU_head;
                    }
                }
                else if(caseMPI == 1 && caseGPU == 1){
                    if ((MPI_head->duration) < (GPU_head->duration)){
                        head = MPI_head;
                    } else{
                        head = GPU_head;
                    }
                }
                else{
                    cout <<"Impossible combination\n";
                }

                cout << "Handling job " << head->jobId << "\n";
                cout << "jobId: " << head->jobId << ", jobType: " << head->jobType << ", k: " << head->k << ", priority: "
                    << head->priority << ", duration: " << head->duration << ", slowDuration: " << head->slowDuration << "\n";
                
                // not enough resources to allocate to head
                if (free_machines < head->k) {
                    cout << "Not enough resources\n";
                    break;
                }
                
                
                // allocate resources according to jobType
                cout << "Prepare to allocate slaves: ";
                switch (head->jobType) {
                    case 2: // JOB_GPU
                        // try to place k containers into gpu machines
                        if ((int32_t)free_racks[0].size() >= head->k) {
                            for (int i = 0; i < head->k; i++) {
                                used_racks[0].push_back(free_racks[0].back());
                                machines.insert(free_racks[0].back());
                                cout << free_racks[0].back() << ", ";
                                free_racks[0].pop_back();
                            }
                        }
                        // not enough gpu machines
                        else {
                            int32_t m = 0;
                            for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                                while(!free_racks[i].empty()) {
                                    used_racks[i].push_back(free_racks[i].back());
                                    machines.insert(free_racks[i].back());
                                    cout << free_racks[i].back() << ", ";
                                    free_racks[i].pop_back();
                                    m++;
                                    if (m >= head->k) break;
                                }
                                if (m >= head->k) break;
                            }
                        }
                        break;
                    case 0: // JOB_MPI
                        // check for available rack to place all k containers
                        for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                            if ((int32_t)free_racks[i].size() >= head->k) {
                                for (int32_t j = 0; j < head->k; j++) {
                                    used_racks[i].push_back(free_racks[i].back());
                                    machines.insert(free_racks[i].back());
                                    cout << free_racks[i].back() << ", ";
                                    free_racks[i].pop_back();
                                }
                                // fast duration
                                duration = head->duration;
                                break;
                            }
                        }
                         // no availble rack
                        if (machines.empty()) {
                            time_t cur_time = time(NULL);
                            if ((cur_time + get_wait_time(cur_time, head->k) + head->duration) <
                                (cur_time + head->slowDuration)) {
                                // wait for free rack
                                break;
                            }
                            // slow duration
                            duration = head->slowDuration;
                            int32_t m = 0;
                            for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                                while(!free_racks[i].empty()) {
                                    used_racks[i].push_back(free_racks[i].back());
                                    machines.insert(free_racks[i].back());
                                    cout << free_racks[i].back() << ", ";
                                    free_racks[i].pop_back();
                                    m++;
                                    if (m >= head->k) break;
                                }
                                if (m >= head->k) break;
                            }
                        }
                        break;
                    default:
                        cout << "(ERROR: Unknown JobType)";
                        break;
                }
                cout << "\n";
                
                if (machines.empty()) {
                    cout << "Delay for free rack\n";
                    break;
                }
                
                free_machines -= (head->k);
                // allocate machines
                client.AllocResources(head->jobId, machines);
                
                cout << "Successfully handled job " << head->jobId << "\n";
                
                // store free time
                time_t cur_time = time(NULL);
                for (std::set<int32_t>::iterator it = machines.begin(); it != machines.end(); ++it)
                    free_times[*it] = cur_time + duration;
                // free job info
                //waiting_jobs.pop();
                if (head->jobType == 0){
                    MPI_jobs.erase(head);
                } else{
                    GPU_jobs.erase(head);
                }
                delete head;
            }
            
            transport->close();
        } catch (TException& tx) {
            printf("ERROR calling YARN : %s\n", tx.what());
            // recover
            printf("Recover: \n");
            for (std::set<int32_t>::iterator it = machines.begin(); it != machines.end(); ++it) {
                int32_t j = 0;
                for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                    j += (int32_t)(free_racks[i].size() + used_racks[i].size());
                    if (*it < j) {
                        vector<int32_t>::iterator it2 = find(used_racks[i].begin(), used_racks[i].end(), *it);
                        if (it2 == used_racks[i].end())
                            cout << "ERROR: Trying to free resource that isnt allocated\n";
                        else {
                            used_racks[i].erase(it2);
                            free_racks[i].push_back(*it);
                            free_machines++;
                        }
                        break;
                    }
                }
                cout << *it  << ", ";
            }
            cout << "\n";
        }
    }
    
public:

    TetrischedServiceHandler(char *config_file)
    {
        // Your initialization goes here
        printf("sjf\n");
        free_machines = 0;
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
                        vector<int32_t> tmp1;
                        used_racks.push_back(tmp1);
                        vector<int32_t> tmp2;
                        // all slaves in a rack are initially free
                        for (int i = free_machines; i < (free_machines + num); i++) {
                            tmp2.push_back(i);
                            cout << i << ",";
                        }
                        cout << "\n";
                        free_racks.push_back(tmp2);
                        free_machines += num;
                    }
                }
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
        
        
        switch (jobType){
            case 2:// JOB_GPU
                GPU_jobs.insert(job);
                break;

            case 0:// JOB_MPI
                MPI_jobs.insert(job);
                break;

            default:
                cout << "(ERROR: Unknown JobType)";
                break;
        }


        // push job to queue
        //waiting_jobs.push(job);
        //cout << "Pushed job " << jobId << " to queue\n";

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
            int32_t j = 0;
            for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                j += (int32_t)(free_racks[i].size() + used_racks[i].size());
                if (*it < j) {
                    vector<int32_t>::iterator it2 = find(used_racks[i].begin(), used_racks[i].end(), *it);
                    if (it2 == used_racks[i].end())
                        cout << "ERROR: Trying to free resource that isnt allocated\n";
                    else {
                        used_racks[i].erase(it2);
                        free_racks[i].push_back(*it);
                        free_machines++;
                    }
                    break;
                }
            }
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

