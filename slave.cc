#include <grpcpp/grpcpp.h>

#include "masterslave.grpc.pb.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <map>
#include <hdfs.h>
#include <fstream>
#include <map>
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterslave::ControlSignalRequest;
using masterslave::ControlSignalResponse;
using masterslave::MasterService;
using masterslave::RegisterSlaveRequest;
using masterslave::RegisterSlaveResponse;
using masterslave::SlaveService;

using masterslave::MapRequest;
using masterslave::MapResponse;
using masterslave::ReduceRequest;
using masterslave::ReduceResponse;

using namespace std;

class Slave : public SlaveService::Service
{
public:
    Status ControlSignal(ServerContext *context, const ControlSignalRequest *request, ControlSignalResponse *response) override
    {
        // cout << "Control Signal Received From Master" << endl;
        return Status::OK;
    }

    Status Map(ServerContext *context, const MapRequest *request, MapResponse *response) override
    {
        string filepath = request->filepath();
        string filename = request->filename();
        int chunkSize = request->chunksize();
        int chunkNumber = request->chunknumber();
        cout << "Map Task Received by Master for File: " << filepath + filename << " on Chunk Number: " << chunkNumber << " with Chunk Size: " << chunkSize << endl;

        hdfsFS fs = hdfsConnect("default", 9870);
        if (fs == NULL)
        {
            cout << "Error while connecting to HDFS..." << endl;
        }

        // Opening input file in HDFS
        hdfsFile input_file = hdfsOpenFile(fs, (filepath + filename).c_str(), O_RDONLY, 0, 0, 0);
        if (!input_file)
        {
            cout << "Failed to open input file " << (filepath + filename) << endl;
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "Failed to open Input File");
        }

        // Opening output file in HDFS
        string map_num = to_string(chunkNumber);
        string outputpath = filepath + "map-" + map_num + ".txt";
        hdfsFile output_file = hdfsOpenFile(fs, outputpath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
        if (!output_file)
        {
            cout << "Failed to open output file " << outputpath << endl;
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "Failed to open Output File");
        }
        else
            cout << "Opened output file successfully: " << outputpath << endl;

        // Initializing required variables for buffer
        bool checkRequired = false; // Check for Case 1 below
        if (chunkNumber != 0)
        {
            hdfsSeek(fs, input_file, chunkNumber * chunkSize - 1);
            bool checkRequired = true;
        }
        int bufferSize = ((1024 > chunkSize) ? chunkSize : 1024) + (checkRequired ? 1 : 0);
        char buffer[bufferSize];
        int bytesRead = 0;
        int totalBytesRead = 0;
        string word = "";

        // Reading a buffer and writing to output file word by word
        while ((bytesRead = hdfsRead(fs, input_file, buffer, bufferSize)) > 0)
        {
            int i = 0;
            // Case 1: if start of chunk is an incomplete word then we skip the incomplete word
            if (checkRequired && bytesRead == bufferSize)
            {
                checkRequired = false;
                while (!(buffer[i] == ' ' || buffer[i] == '\n'))
                    i++;
                i++;
                bytesRead--; // Cause we added 1 for Checking previous letter of start of chunk
            }
            for (; i < bytesRead; i++)
            {
                if ((buffer[i] == ' ' || buffer[i] == '\n') && word != "")
                {
                    hdfsWrite(fs, output_file, word.c_str(), word.size());
                    hdfsWrite(fs, output_file, "\n", 1);
                    // cout << word << endl;
                    word.clear();
                    checkRequired = false;
                }
                else
                {
                    word += buffer[i];
                    checkRequired = true;
                }
            }

            totalBytesRead += bytesRead;
            // Case 2: if end of chunk is incomplete word then we complete reading the word

            if (checkRequired && totalBytesRead == chunkSize && word != "")
            {
                char c;
                while (hdfsAvailable(fs, input_file) > 0)
                {
                    bytesRead = hdfsRead(fs, input_file, &c, 1);
                    if (!(c == ' ' || c == '\n'))
                        word += buffer[i];
                    else
                        break;
                }
                hdfsWrite(fs, output_file, word.c_str(), word.size());
                hdfsWrite(fs, output_file, "\n", 1);
            }

            // Checking if 1024 is larger than the remaining chunk for Slave
            bufferSize = ((1024 > chunkSize - totalBytesRead) ? chunkSize - totalBytesRead : 1024);
        }

        // Close files and disconnect from HDFS
        hdfsCloseFile(fs, input_file);
        hdfsCloseFile(fs, output_file);
        hdfsDisconnect(fs);

        cout << "Map Task Completed on Chunk number:" << chunkNumber << endl;
        return Status::OK;
    }

    bool isMyKey(string word, string keyrange)
    {
        for (int i = 0; i < keyrange.size(); i++)
        {
            if (word[0] == keyrange[i])
                return true;
        }
        return false;
    }

    Status Reduce(ServerContext *context, const ReduceRequest *request, ReduceResponse *response) override
    {
        string maplocation = request->maplocation();
        int numofmaps = request->numofmaps();
        string keyrange = request->keyrange();

        cout << "Reduce Task Received by Master on Map Location" << maplocation << " with " << numofmaps << " Maps " << endl;
        map<string, int> wordcount;

        hdfsFS fs = hdfsConnect("default", 9870);
        if (fs == NULL)
        {
            cout << "Error while connecting to HDFS..." << endl;
        }

        string outputfile = maplocation + "output-" + keyrange[0] + ".txt";

        string mapprefix = "map-";
        for (int i = 0; i < numofmaps; i++)
        {
            string filename = maplocation + mapprefix + to_string(i) + ".txt";
            hdfsFile input_file = hdfsOpenFile(fs, filename.c_str(), O_RDONLY, 0, 0, 0);
            if (!input_file)
            {
                cout << "Failed to open input file " << filename << endl;
                return Status(grpc::StatusCode::FAILED_PRECONDITION, "Failed to open Input File");
            }

            int bytesRead;
            char buffer[1024];
            string word;
            while ((bytesRead = hdfsRead(fs, input_file, buffer, 1024)) > 0)
            {
                for (int i = 0; i < bytesRead; i++)
                {
                    if (buffer[i] == '\n')
                    {
                        if (!word.empty() && isMyKey(word, keyrange))
                        {
                            wordcount[word]++;
                        }
                        word.clear();
                    }
                    else
                    {
                        word += buffer[i];
                    }
                }
            }
            cout << "File Read: " << filename << endl;
            hdfsCloseFile(fs, input_file);
        }

        hdfsFile output_file = hdfsOpenFile(fs, outputfile.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
        if (!output_file)
        {
            cout << "Failed to open output file " << outputfile << endl;
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "Failed to open Output File");
        }

        for (auto &word : wordcount)
        {
            cout << word.first << " " << word.second << endl;
            hdfsWrite(fs, output_file, word.first.c_str(), word.first.size());
            hdfsWrite(fs, output_file, " ", 1);
            hdfsWrite(fs, output_file, (to_string(word.second)).c_str(), to_string(word.second).size());
            hdfsWrite(fs, output_file, "\n", 1);
        }

        hdfsCloseFile(fs, output_file);
        hdfsDisconnect(fs);

        cout << "Reduce Task Completed Output Stored To: " << outputfile << endl;
        return Status::OK;
    }

    void RegisterWithMaster(string addr)
    {
        auto channel = grpc::CreateChannel("0.0.0.0:50056", grpc::InsecureChannelCredentials());
        auto stub = MasterService::NewStub(channel);
        RegisterSlaveRequest request;
        RegisterSlaveResponse response;
        request.set_address(addr);
        ClientContext context;
        auto status = stub->RegisterSlave(&context, request, &response);
        if (status.ok())
        {
            cout << "The Slave has been registered with Master with Address: " << addr << endl;
        }
        else
        {
            cout << "Error while registering with Master on:" << addr << endl;
            exit(1);
        }
    }
};

int main(int argc, char **argv)
{
    setenv("CLASSPATH", "/home/sabooh/hadoop-3.3.5/etc/hadoop:/home/sabooh/hadoop-3.3.5/share/hadoop/common/*:/home/sabooh/hadoop-3.3.5/share/hadoop/common/lib/*:/home/sabooh/hadoop-3.3.5/share/hadoop/hdfs/*:/home/sabooh/hadoop-3.3.5/share/hadoop/hdfs/lib/*:/home/sabooh/hadoop-3.3.5/share/hadoop/mapreduce/*:/home/sabooh/hadoop-3.3.5/share/hadoop/mapreduce/lib/*", 1);
    string port;
    if (argc == 2)
        port = argv[1];
    else
    {
        cout << "Enter Port Number for this Slave: ";
        cin >> port;
    }
    string server_address("0.0.0.0:" + port);
    Slave service;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    unique_ptr<grpc::Server> server(builder.BuildAndStart());
    service.RegisterWithMaster(server_address);
    cout << "Server listening on " << server_address << endl;
    server->Wait();
    return 0;
}