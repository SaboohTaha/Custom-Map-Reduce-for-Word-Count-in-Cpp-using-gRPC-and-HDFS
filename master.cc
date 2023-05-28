#include <grpcpp/grpcpp.h>
#include "masterslave.grpc.pb.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include <algorithm>
#include <hdfs.h>
#include <string>
#include <sstream>

using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterslave::ControlSignalRequest;
using masterslave::ControlSignalResponse;
using masterslave::MasterService;
using masterslave::QuerySlaveStatusRequest;
using masterslave::QuerySlaveStatusResponse;
using masterslave::RegisterSlaveRequest;
using masterslave::RegisterSlaveResponse;
using masterslave::SlaveService;
using masterslave::UpdateControlIntervalRequest;
using masterslave::UpdateControlIntervalResponse;

using masterslave::MapRequest;
using masterslave::MapResponse;
using masterslave::ReduceRequest;
using masterslave::ReduceResponse;

using namespace std;

struct Slave
{
    string address;
    bool responsive;
    bool isFree;
    Status SignalStatus;
    Status TaskStatus;
};

class Master : public MasterService::Service
{
    chrono::seconds controlInt;
    chrono::seconds timeoutInt;
    int noOfSlaves;
    map<int, Slave> Slaves;

public:
    Master() : controlInt(chrono::seconds(1)), timeoutInt(chrono::seconds(4)), noOfSlaves(0) {}

    // RPC Call for Updating Control Interval (Implemented in Assignment 2)
    Status UpdateControlInterval(ServerContext *context, const UpdateControlIntervalRequest *request, UpdateControlIntervalResponse *response) override
    {
        // Updating Control Interval and Timeout Interval Duration
        controlInt = chrono::seconds(request->interval());
        timeoutInt = chrono::seconds(request->interval() * 4);
        response->set_success(true);
        return Status::OK;
    }

    // Local function for updating Control Interval
    void UpdateControlInterval(int duration)
    {
        controlInt = chrono::seconds(duration);
        timeoutInt = chrono::seconds(duration * 4);
        return;
    }

    // RPC call for checking the status of Slaves by comparing their Addresses and checking its responsive variable
    Status QuerySlaveStatus(ServerContext *context, const QuerySlaveStatusRequest *request, QuerySlaveStatusResponse *response) override
    {
        string slaveAddress = request->address();
        auto slavesItr = Slaves.begin();
        response->set_responsive(false);
        while (slavesItr != Slaves.end())
        {
            if (slavesItr->second.address == slaveAddress)
            {
                response->set_responsive(slavesItr->second.responsive);
                break;
            }
        }
        return Status::OK;
    }

    // Whenever a slave is created, it calls this RPC to register itself with this master
    Status RegisterSlave(ServerContext *context, const RegisterSlaveRequest *request, RegisterSlaveResponse *response) override
    {
        // Adding Slave to the Slaves Map and Responding whether it is successfully added
        string addr = request->address();
        Slaves[noOfSlaves] = {addr, true, true};
        ++noOfSlaves;
        response->set_success(true);
        return Status::OK;
    }

    // Sending control signals to check whether the registered slaves are resposive or not
    void SendControlSignals()
    {
        while (true)
        {
            // Sending Control Intervals to Each Slave
            for (auto &slave : Slaves)
            {
                auto channel = grpc::CreateChannel(slave.second.address, grpc::InsecureChannelCredentials());
                auto stub = SlaveService::NewStub(channel);
                ControlSignalRequest request;
                ControlSignalResponse response;
                ClientContext context;
                slave.second.SignalStatus = stub->ControlSignal(&context, request, &response);
            }

            // Sleeping for timeout interval
            this_thread::sleep_for(timeoutInt);

            // Checking Responses of Each Slave to Mark Responsiveness
            for (auto &slave : Slaves)
            {
                if (slave.second.SignalStatus.ok())
                {
                    slave.second.responsive = true;
                }
                else
                {
                    slave.second.responsive = false;
                    // cout << "Slave: " << slave.first << " " << slave.second.address << " has become unresponsive" << endl;
                }
            }

            // Sleeping for the delay of Control Interval
            this_thread::sleep_for(controlInt);
        }
    }

    void PrintSlaveStatus()
    {
        cout << "-----------------------------------------------------------------------" << endl;
        for (auto &slave : Slaves)
        {
            cout << "Slave: " << slave.first << " with Address: " << slave.second.address << " is " << (slave.second.responsive ? "Responsive" : "UNRESPONSIVE") << endl;
        }
        cout << "=======================================================================" << endl;
    }

    void SendMapTask(int SlaveID, string filename, string filepath, int chunkSize, int chunkNumber, vector<pair<bool, bool>> &taskCompletion)
    {
        auto channel = grpc::CreateChannel(Slaves[SlaveID].address, grpc::InsecureChannelCredentials());
        auto stub = SlaveService::NewStub(channel);
        MapRequest request;
        request.set_filepath(filepath);
        request.set_filename(filename);
        request.set_chunksize(chunkSize);
        request.set_chunknumber(chunkNumber);
        MapResponse response;
        ClientContext context;
        Slaves[SlaveID].TaskStatus = stub->Map(&context, request, &response);
        Slaves[SlaveID].isFree = true;
        if (Slaves[SlaveID].TaskStatus.ok())
        {
            taskCompletion[chunkNumber].first = true;
            cout << "Map task has been completed by Slave:" << Slaves[SlaveID].address << endl;
            PrintMapCompletion(taskCompletion);
        }
        else
        {
            cout << "Map Task failed by Slave:" << Slaves[SlaveID].address << " Error:" << Slaves[SlaveID].TaskStatus.error_message() << endl;
            taskCompletion[chunkNumber].second = false;
        }
    }

    void PrintMapCompletion(const vector<pair<bool, bool>> &taskCompletion)
    {
        int total = taskCompletion.size();
        int completed = 0;
        for (int i = 0; i < total; i++)
        {
            if (taskCompletion[i].first)
                ++completed;
        }
        cout << "Map Task Completion: " << (completed * 100 / total) << "%" << endl;
    }

    int AssignMapTasks()
    {
        hdfsFS fs = hdfsConnect("default", 9870);
        if (fs == NULL)
        {
            cout << "Error while connecting to HDFS..." << endl;
        }
        string filename = "US_AirLines.txt";
        string filepath = "/files/";

        // Getting File Info
        hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, (filepath + filename).c_str());
        if (!fileInfo)
        {
            cout << "Failed to get file info for " << filename << endl;
            hdfsDisconnect(fs);
        }

        // Getting File Size
        tOffset fileSize = fileInfo->mSize;
        cout << "Size of " << filename << " is " << fileSize << " bytes" << endl;

        hdfsFreeFileInfo(fileInfo, 1);
        hdfsDisconnect(fs);
        // Printing Division of Tasks
        int divisionSize;
        int noOfMapTasks = noOfSlaves;
        if (noOfMapTasks > 0)
        {
            divisionSize = fileSize / noOfMapTasks;
            cout << "Each task is divided into " << divisionSize << " byte size. With total of " << noOfMapTasks << " Tasks. Division is bases on no of Slaves registered with Master." << endl
                 << endl;
        }
        else
        {
            cout << "There is no Slave to Assign tasks to. Returning without completing task." << endl;
            hdfsFreeFileInfo(fileInfo, 1);
            hdfsDisconnect(fs);
            return noOfMapTasks;
        }

        // Initializing required variables
        vector<pair<bool, bool>> taskCompletion(noOfMapTasks, {false, false});

        // Assigning Tasks to all slaves && Reassigning Unassigned
        while (true)
        {
            bool allComplete = true;
            for (int i = 0; i < noOfMapTasks; i++)
            {
                bool isaSlaveFree = false;
                // .first if the task has not been completed & .second if the task has not been sent to a slave
                if (!taskCompletion[i].first && !taskCompletion[i].second)
                {
                    for (auto &slave : Slaves)
                    {
                        if (slave.second.responsive && slave.second.isFree)
                        {
                            isaSlaveFree = true;
                            // SendMapTask(slave.first, filename, filepath, divisionSize, i, taskCompletion);
                            slave.second.isFree = false;
                            thread thx(&Master::SendMapTask, this, slave.first, filename, filepath, divisionSize, i, ref(taskCompletion));
                            taskCompletion[i].second = true;
                            cout << "Map Task with Chunk Number " << i << " Sent to Slave: " << slave.second.address << endl;
                            thx.detach();
                            break;
                        }
                    }
                }
                // If it did not found a slave to give a task wait for a second and then proceed
                if (!isaSlaveFree)
                    this_thread::sleep_for(chrono::seconds(1));
                // If a task is left for completion mark allComplete to be false
                if (!taskCompletion[i].first)
                    allComplete = false;
            }
            if (allComplete)
                break;
        }
        cout << "All Map Tasks has been completed!" << endl;
        return noOfMapTasks;
    }

    void SendReduceTask(int SlaveID, string mapPath, string key, int numofMaps, int reduceID, vector<pair<bool, bool>> &taskCompletion)
    {
        auto channel = grpc::CreateChannel(Slaves[SlaveID].address, grpc::InsecureChannelCredentials());
        auto stub = SlaveService::NewStub(channel);
        ReduceRequest request;
        request.set_maplocation(mapPath);
        request.set_numofmaps(numofMaps);
        request.set_keyrange(key);
        ReduceResponse response;
        ClientContext context;
        Slaves[SlaveID].TaskStatus = stub->Reduce(&context, request, &response);
        Slaves[SlaveID].isFree = true;
        if (Slaves[SlaveID].TaskStatus.ok())
        {
            taskCompletion[reduceID].first = true;
            cout << "Reduce task has been completed by Slave:" << Slaves[SlaveID].address << endl;
            PrintReduceCompletion(taskCompletion);
        }
        else
        {
            cout << "Reduce Task failed by Slave:" << Slaves[SlaveID].address << " Error:" << Slaves[SlaveID].TaskStatus.error_message() << endl;
            taskCompletion[reduceID].second = false;
        }
    }

    void PrintReduceCompletion(const vector<pair<bool, bool>> &taskCompletion)
    {
        int total = taskCompletion.size();
        int completed = 0;
        for (int i = 0; i < total; i++)
        {
            if (taskCompletion[i].first)
                ++completed;
        }
        cout << "Reduce Task Completion: " << (completed * 100 / total) << "%" << endl;
    }

    string AssignReduceTasks(int numOfMaps)
    {
        string maplocation = "/files/";
        string key;
        for (int i = 97; i <= 122; i++)
            key += char(i);
        vector<string> keyranges;
        int letters = key.size();
        int divisionSize = letters / numOfMaps;
        bool unequalDivision = letters % numOfMaps;
        // dividing key into a vector of key ranges
        for (int i = 0; i < letters; i += divisionSize)
        {
            if (i + divisionSize < letters || !unequalDivision)
                keyranges.push_back(key.substr(i, divisionSize));
            else
                keyranges[keyranges.size() - 1] += key.substr(i, divisionSize);
        }

        // Output files are named as output-(the first key from keyrange).txt So getting these first key letter for use in Sorting
        string keysForSorting;
        for (int i = 0; i < keyranges.size(); i++)
        {
            keysForSorting += keyranges[i].at(0);
        }

        // Initializing required variables
        vector<pair<bool, bool>> taskCompletion(numOfMaps, {false, false});

        // Assigning Tasks to all slaves && Reassigning Unassigned
        while (true)
        {
            bool allComplete = true;
            for (int i = 0; i < keyranges.size(); i++)
            {
                bool isaSlaveFree = false;
                // .first if the task has not been completed & .second if the task has not been sent to a slave
                if (!taskCompletion[i].first && !taskCompletion[i].second)
                {
                    for (auto &slave : Slaves)
                    {
                        if (slave.second.responsive && slave.second.isFree)
                        {
                            isaSlaveFree = true;
                            slave.second.isFree = false;
                            thread thx(&Master::SendReduceTask, this, slave.first, maplocation, keyranges[i], numOfMaps, i, ref(taskCompletion));
                            taskCompletion[i].second = true;
                            cout << "Reduce Task " << i << " Sent to Slave: " << slave.second.address << " With Keys: " << keyranges[i] << endl;
                            thx.detach();
                            break;
                        }
                    }
                }
                // If it did not found a slave to give a task wait for a second and then proceed
                if (!isaSlaveFree)
                    this_thread::sleep_for(chrono::seconds(1));
                // If a task is left for completion mark allComplete to be false
                if (!taskCompletion[i].first)
                    allComplete = false;
            }
            if (allComplete)
                break;
        }
        cout << "All Reduce Tasks has been completed!" << endl;
        return keysForSorting;
    }

    int getline(string &line, char *buffer, const int bytesRead, const int bufferRead, bool &isFullLine)
    {
        int read = 0;
        isFullLine = false;
        for (int i = bufferRead; i < bytesRead; i++)
        {
            if (buffer[i] == '\n')
            {
                isFullLine = true;
                return read;
            }
            else
            {
                line += buffer[i];
                ++read;
            }
        }
        return read;
    }

    void PrintTopKWords(string keysForSorting)
    {
        hdfsFS fs = hdfsConnect("default", 9870);
        if (fs == NULL)
        {
            cout << "Error while connecting to HDFS..." << endl;
        }
        string path = "/files/";
        string fileprefix = "output-";

        map<int, string> word_counts;
        for (int i = 0; i < keysForSorting.size(); i++)
        {
            string fileloc = path + fileprefix + keysForSorting[i] + ".txt";
            hdfsFile file = hdfsOpenFile(fs, fileloc.c_str(), O_RDONLY, 0, 0, 0);
            if (!file)
            {
                cout << "Failed to open file " << fileloc << endl;
                continue;
            }
            char buffer[1024];
            int bytesRead = 0;
            while ((bytesRead = hdfsRead(fs, file, buffer, 1024)) > 0)
            {
                int bufferRead = 0;
                string line;
                bool isFullLine = false;
                bufferRead += getline(line, buffer, bytesRead, bufferRead, isFullLine);
                while (isFullLine)
                {
                    if (!line.empty())
                    {
                        string word;
                        int count;
                        stringstream lineStream(line);
                        lineStream >> word >> count;
                        word_counts[count] = word;
                    }
                    bufferRead += getline(line, buffer, bytesRead, bufferRead, isFullLine);
                }
            }
            hdfsCloseFile(fs, file);
        }
        int k;
        cout << "Enter value of K: ";
        cin >> k;
        int i = 0;

        hdfsDisconnect(fs);
        for (map<int, string>::reverse_iterator itr = word_counts.rbegin(); itr != word_counts.rend(); itr++)
        {
            if (i == k)
            {
                break;
            }
            cout << itr->second << " " << itr->first << endl;
            i++;
        }
    }

    void Interface()
    {
        int option;
        while (true)
        {
            cout << "1. For Seeing All Slaves Status." << endl;
            cout << "2. To Give Map Reduce Tasks To Slaves." << endl;
            cout << "3. To Change the Control Interval." << endl;
            cout << "4. To Reprint the Interface With Clearing the Screen." << endl;
            cout << "5. To Close the Server and Exit" << endl;
            cin >> option;
            if (option == 1)
            {
                PrintSlaveStatus();
            }
            else if (option == 2)
            {
                int numOfMaps = AssignMapTasks();
                if (numOfMaps > 0)
                {
                    string keyForSorting = AssignReduceTasks(numOfMaps);
                    PrintTopKWords(keyForSorting);
                }
                else
                    cout << "There is no Slave to give Map Task to." << endl;
            }
            else if (option == 3)
            {
                int interval;
                cout << "Enter the new value for Interval: ";
                cin >> interval;
                UpdateControlInterval(interval);
            }
            else if (option == 4)
            {
                system("clear");
            }
            else if (option == 5)
            {
                cout << "Shutting Down The Server by Killing the Process." << endl;
                exit(0);
            }
            else
            {
                cout << "Incorrect Option Selected. Select Again!" << endl;
            }
        }
    }
};

int main(int argc, char **argv)
{
    setenv("CLASSPATH", "/home/sabooh/hadoop-3.3.5/etc/hadoop:/home/sabooh/hadoop-3.3.5/share/hadoop/common/*:/home/sabooh/hadoop-3.3.5/share/hadoop/common/lib/*:/home/sabooh/hadoop-3.3.5/share/hadoop/hdfs/*:/home/sabooh/hadoop-3.3.5/share/hadoop/hdfs/lib/*:/home/sabooh/hadoop-3.3.5/share/hadoop/mapreduce/*:/home/sabooh/hadoop-3.3.5/share/hadoop/mapreduce/lib/*", 1);
    Master master;
    ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50056", grpc::InsecureServerCredentials());
    builder.RegisterService(&master);
    unique_ptr<Server> server(builder.BuildAndStart());
    cout << "Server listening on 0.0.0.0:50056" << endl;

    // thread control_thread(&Master::SendControlSignals, &master);
    thread interface_thread(&Master::Interface, &master);
    server->Wait();
    return 0;
}