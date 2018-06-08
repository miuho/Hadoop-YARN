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

#define NONE 1
#define HARD 2
#define SOFT 3
#define MAX_WAIT_TIME 1200

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
    double chosen_duration;
    time_t added_time;
    int32_t *machines;
};

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{

private:
    int free_machines;
    int mode; // 1 = none, 2 = hard, 3 = soft
    vector<vector<int32_t>> free_racks;
    vector<vector<int32_t>> used_racks;
    unordered_map<int32_t, time_t> free_times;
    set<Job_S *> jobs;
    mutex mtx;
    
    Job_S *get_random_job(void) {
        // find the earliest added job (fifo)
        int earliest_time = INT_MAX;
        Job_S *earliest_job = NULL;
        for (std::set<Job_S *>::iterator it = jobs.begin(); it != jobs.end(); ++it) {
            Job_S *job = *it;
            if (job->added_time < earliest_time) {
                earliest_time = job->added_time;
                earliest_job = job;
            }
        }
        // check if possible to allocate at all
        if (free_machines < earliest_job->k)
            return NULL;
        // randomly allocate machines
        srand (time(NULL));
        int j = 0;
        while (j < earliest_job->k) {
            int rand_i = rand() % free_racks.size();
            if (free_racks[rand_i].empty())
                continue;
            used_racks[rand_i].push_back(free_racks[rand_i].back());
            (earliest_job->machines)[j] = free_racks[rand_i].back(); // overwrite each check
            free_racks[rand_i].pop_back();
            j++;
        }
        return earliest_job;
    }
    
    time_t get_waiting_time_gpu(time_t cur_time, int32_t k) {
        if (k > (int32_t)(free_racks[0].size() + used_racks[0].size())) {
            cout << "Invalid GPU rack\n";
            return cur_time;
        }
        // compute the minimum waiting time for gpu rack
        int32_t require = k - free_racks[0].size();
        vector<time_t> times;
        for (int32_t j = 0; j < (int32_t)used_racks[0].size(); j++) {
            time_t t = free_times[used_racks[0][j]];
            time_t diff = (t > cur_time) ? (t - cur_time) : 0;
            times.push_back(diff);
        }
        // sort the waiting time
        sort(times.begin(), times.end());
        time_t rack_min = times[require - 1];
        return rack_min;
    }
    
    time_t get_waiting_time_mpi(time_t cur_time, int32_t k) {
        time_t global_rack_min = cur_time;
        // traverse all racks
        for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
            // skips those small racks
            if (k > (int32_t)(free_racks[i].size() + used_racks[i].size()))
                continue;
            // compute the minimum waiting time for this rack
            int32_t require = k - free_racks[i].size();
            if (require <= 0) {
                cout << "ERROR: Found full rack to allocate\n";
                continue;
            }
            vector<time_t> times;
            for (int32_t j = 0; j < (int32_t)used_racks[i].size(); j++) {
                time_t t = free_times[used_racks[i][j]];
                time_t diff = (t > cur_time) ? (t - cur_time) : 0;
                times.push_back(diff);
            }
            // sort the waiting time
            sort(times.begin(), times.end());
            time_t rack_min = times[require - 1];
            if (rack_min < global_rack_min)
                global_rack_min = rack_min;
        }
        return global_rack_min;
    }
    
    int get_utility(Job_S *job, bool preferred) {
        int utility = 1200;
        int waited_time = (int)(job->added_time - time(NULL));
        if (preferred)
            utility -= (waited_time + job->duration);
        else
            utility -= (waited_time + job->slowDuration);
        return (utility < 0) ? 0 : utility;
    }
    
    Job_S *get_max_utility_job(time_t cur_time) {
        int max_utility = -1;
        Job_S *max_utility_job = NULL;
        cout << "Traverse all jobs\n";
        for (std::set<Job_S *>::iterator it = jobs.begin(); it != jobs.end(); ++it) {
            Job_S *job = *it;
            // check if possible to allocate at all
            if (free_machines < job->k)
                continue;
            bool preferred = false;
            if (job->jobType == 0) { // JOB_MPI
                cout << "Looking at MPI job " << job->jobId << "\n";
                // check for available non-gpu racks to place all k containers
                for (int32_t i = 1; i < (int32_t)free_racks.size(); i++) {
                    if ((int32_t)free_racks[i].size() >= job->k) {
                        preferred = true;
                        cout << "Found full rack: ";
                        for (int32_t j = 0; j < job->k; j++) {
                            //used_racks[i].push_back(free_racks[i].back());
                            //(job->machines)[j] = free_racks[i].back(); // overwrite each check
                            //cout << j << ":" <<  free_racks[i].back() << ",";
                            //free_racks[i].pop_back();
                            (job->machines)[j] = free_racks[i][j];
                            cout << free_racks[i][j] << ",";
                        }
                        cout << "\n";
                        // fast duration
                        job->chosen_duration = job->duration;
                        int cur_utility = get_utility(job, preferred);
                        if (cur_utility > max_utility) {
                            max_utility = cur_utility;
                            max_utility_job = job;
                        }
                        break;
                    }
                }
                // check if gpu rack is available
                if (!preferred) {
                    // try to place k containers into gpu machines
                    if ((int32_t)free_racks[0].size() >= job->k) {
                        preferred = true;
                        cout << "Finding in GPU rack: ";
                        for (int i = 0; i < job->k; i++) {
                            //used_racks[0].push_back(free_racks[0].back());
                            //(job->machines)[i] = free_racks[0].back(); // overwrite each check
                            //cout << i << ":" <<  free_racks[0].back() << ",";
                            //free_racks[0].pop_back();
                            (job->machines)[i] = free_racks[0][i];
                            cout << free_racks[0][i] << ",";
                        }
                        cout << "\n";
                        // fast duration
                        job->chosen_duration = job->duration;
                        int cur_utility = get_utility(job, preferred);
                        if (cur_utility > max_utility) {
                            max_utility = cur_utility;
                            max_utility_job = job;
                        }
                    }
                }
                // no availble full rack
                if (!preferred && mode == SOFT) {
                    cout << "Cannot find full rack\n";
                    if ((cur_time + get_waiting_time_mpi(cur_time, job->k) + job->duration) <
                        (cur_time + job->slowDuration)) {
                        // wait for free rack
                        //cout << "Delay for free rack\n";
                        continue;
                    }
                    cout << "Finding in other racks: ";
                    int32_t m = 0;
                    for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                        //while(!free_racks[i].empty()) {
                        for (int32_t j = 0; j < (int32_t)free_racks[i].size(); j++) {
                            //used_racks[i].push_back(free_racks[i].back());
                            //(job->machines)[m] = free_racks[i].back(); // overwrite each check
                            //cout << m << ":" << free_racks[i].back() << ",";
                            //free_racks[i].pop_back();
                            (job->machines)[m] = free_racks[i][j];
                            cout << free_racks[i][j] << ",";
                            m++;
                            if (m >= job->k) break;
                        }
                        if (m >= job->k) break;
                    }
                    cout << "\n";
                    // slow duration
                    job->chosen_duration = job->slowDuration;
                    int cur_utility = get_utility(job, preferred);
                    if (cur_utility > max_utility) {
                        max_utility = cur_utility;
                        max_utility_job = job;
                    }
                }
            }
            else if (job->jobType == 2) { // JOB_GPU
                cout << "Looking at GPU job " << job->jobId << "\n";
                // try to place k containers into gpu machines
                if ((int32_t)free_racks[0].size() >= job->k) {
                    preferred = true;
                    cout << "Finding in GPU rack: ";
                    for (int i = 0; i < job->k; i++) {
                        //used_racks[0].push_back(free_racks[0].back());
                        //(job->machines)[i] = free_racks[0].back(); // overwrite each check
                        //cout << i << ":" <<  free_racks[0].back() << ",";
                        //free_racks[0].pop_back();
                        (job->machines)[i] = free_racks[0][i];
                        cout << free_racks[0][i] << ",";
                    }
                    cout << "\n";
                    // fast duration
                    job->chosen_duration = job->duration;
                    int cur_utility = get_utility(job, preferred);
                    if (cur_utility > max_utility) {
                        max_utility = cur_utility;
                        max_utility_job = job;
                    }
                }
                // not enough gpu machines
                if (!preferred && mode == SOFT) {
                    if ((cur_time + get_waiting_time_gpu(cur_time, job->k) + job->duration) <
                        (cur_time + job->slowDuration)) {
                        // wait for free gpu rack
                        //cout << "Delay for free gpu rack\n";
                        continue;
                    }
                    cout << "Finding in other racks: ";
                    int32_t m = 0;
                    for (int32_t i = 0; i < (int32_t)free_racks.size(); i++) {
                        //while(!free_racks[i].empty()) {
                        for (int32_t j = 0; j < (int32_t)free_racks[i].size(); j++) {
                            //used_racks[i].push_back(free_racks[i].back());
                            //(job->machines)[m] = free_racks[i].back(); // overwrite each check
                            //cout << m << ":" << free_racks[i].back() << ",";
                            //free_racks[i].pop_back();
                            (job->machines)[m] = free_racks[i][j];
                            cout << free_racks[i][j] << ",";
                            m++;
                            if (m >= job->k) break;
                        }
                        if (m >= job->k) break;
                    }
                    cout << "\n";
                    // slow duration
                    job->chosen_duration = job->slowDuration;
                    int cur_utility = get_utility(job, preferred);
                    if (cur_utility > max_utility) {
                        max_utility = cur_utility;
                        max_utility_job = job;
                    }
                }
            }
            else {
                cout << "Invalid jobType\n";
            }
        }
        if (max_utility_job != NULL) {
            for (int32_t i = 0; i < max_utility_job->k; i++) {
                int32_t machine = (max_utility_job->machines)[i];
                int32_t j = 0;
                for (int32_t k = 0; k < (int32_t)free_racks.size(); k++) {
                    j += (int32_t)(free_racks[k].size() + used_racks[k].size());
                    if (machine < j) {
                        vector<int32_t>::iterator it2 = find(free_racks[k].begin(), free_racks[k].end(), machine);
                        if (it2 == free_racks[k].end())
                            cout << "ERROR: Trying to allocate resource that is already allocated\n";
                        else {
                            free_racks[k].erase(it2);
                            used_racks[k].push_back(machine);
                        }
                        break;
                    }
                }
            }
        }
        return max_utility_job;
    }
    
    void TryToAllocate() {
        cout << "TryToAllocate\n";
        // remove jobs that waited too long
        time_t cur_time = time(NULL);
        for (std::set<Job_S *>::iterator it = jobs.begin(); it != jobs.end(); ) {
            Job_S *job = *it;
            cout << "job " << job->jobId << " waited for " << (cur_time - job->added_time) << " seconds\n";
            // max allowable waiting time in job list < already waited time in job list
            if ((MAX_WAIT_TIME - job->duration) < (cur_time - job->added_time)) {
                cout << "Removed timeout job: " << job->jobId << "\n";
                it = jobs.erase(it);
                delete [] (job->machines);
                delete job;
            }
            else {
                ++it;
            }
        }
        
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
            while (!jobs.empty()) {
                cout << "Begin to find a job...\n";
                cur_time = time(NULL);
                machines.clear();

                Job_S *head = NULL;
                if (mode == NONE)
                    head = get_random_job();
                else
                    head = get_max_utility_job(cur_time);
                    
                // not enough resources to allocate to head
                if (head == NULL) {
                    cout << "Not enough resources\n";
                    break;
                }
                cout << "Got resources\n";
                
                cout << "Handling job " << head->jobId << "\n";
                
                // allocate resources
                cout << "Prepare to allocate slaves: ";
                for (int i = 0; i < (head->k); i++) {
                    int32_t machine = (head->machines)[i];
                    cout << machine << ", ";
                    machines.insert(machine);
                }
                cout << "\n";
                
                free_machines -= (head->k);
                // allocate machines
                client.AllocResources(head->jobId, machines);
                
                cout << "Successfully handled job " << head->jobId << "\n";
                cout << "k: " << head->k << " chosen_duration: " << head->chosen_duration << "\n";
                
                // store free time
                for (std::set<int32_t>::iterator it = machines.begin(); it != machines.end(); ++it)
                    free_times[*it] = cur_time + (head->chosen_duration);
                // free job info
                delete [] (head->machines);
                jobs.erase(jobs.find(head));
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
        printf("init\n");
        free_machines = 0;
        mode = 0; // 1 = none, 2 = hard, 3 = soft
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
                        size_t found = line.find("simtype");
                        if (found != string::npos) {
                            string tmp = line.substr(found + 8);
                            found = tmp.find("\"");
                            if (found != string::npos) {
                                tmp = tmp.substr(found + 1);
                                found = tmp.find("\"");
                                if (found != string::npos) {
                                    tmp = tmp.substr(0, found);
                                    cout << tmp << "\n";
                                    if (!tmp.compare("none"))
                                        mode = NONE;
                                    else if (!tmp.compare("hard"))
                                        mode = HARD;
                                    else if (!tmp.compare("soft"))
                                        mode = SOFT;
                                    else
                                        cout << "Invalid simtype\n";
                                    cout << "mode: " << mode << "\n";
                                }
                            }
                        }
                        found = line.find("rack_cap");
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
                }
                in.close();
                if (!mode) {
                    printf("No specified policy: using SOFT as default\n");
                    mode = SOFT; // default is soft
                }
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
        cout << "AddJob\n";
        cout << "jobId: " << jobId << ", jobType: " << jobType << ", k: " << k << ", priority: "
            << priority << ", duration: " << duration << ", slowDuration: " << slowDuration << "\n";
        
        // create Job_S structure to store job info
        Job_S *job  = new Job_S;
        job->jobId = jobId;
        job->jobType = jobType;
        job->k = k;
        job->priority = priority;
        job->duration = duration;
        job->slowDuration = slowDuration;
        job->added_time = time(NULL);
        job->machines = new int32_t[k];
        
        jobs.insert(job);

        TryToAllocate();

        mtx.unlock();
    }

    void FreeResources(const std::set<int32_t> & machines)
    {
        mtx.lock();
        cout << "FreeResources\n";
        
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
    if (argc == 2)
        config_file = argv[1];
    else if (argc == 3)
        config_file = argv[2];
    else
        printf("ERROR: no config file\n");
    boost::shared_ptr<TetrischedServiceHandler> handler(new TetrischedServiceHandler(config_file));
    boost::shared_ptr<TProcessor> processor(new TetrischedServiceProcessor(handler));
    boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(alschedport));
    boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
