#ifndef IO_H_
#define IO_H_

#include "hdfs.h"
#include <cstring>
#include <cstdlib> //realloc
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include "global.h"

#define HDFS_BUF_SIZE 65536
#define LINE_DEFAULT_SIZE 4096
#define HDFS_BLOCK_SIZE 8388608 //8M
//====== get File System ======
hdfsFS getHdfsFS()
{
    hdfsFS fs = hdfsConnect("master", 9000);
    if (!fs) {
        fprintf(stderr, "Failed to connect to HDFS!\n");
        exit(-1);
    }
    return fs;
}

hdfsFS getlocalFS()
{
    hdfsFS lfs = hdfsConnect(NULL, 0);
    if (!lfs) {
        fprintf(stderr, "Failed to connect to 'local' FS!\n");
        exit(-1);
    }
    return lfs;
}

//====== get File Handle ======

hdfsFile getRHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_RDONLY | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s for reading!\n", path);
        exit(-1);
    }
    return hdl;
}

hdfsFile getWHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_WRONLY | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s for writing!\n", path);
        exit(-1);
    }
    return hdl;
}

hdfsFile getRWHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_RDWR | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s!\n", path);
        exit(-1);
    }
    return hdl;
}

/**
 *  \brief create a directory on hdfs in the given path
 *  \param [in] directory path
 */

int dirCreate(const char* path)
{
    hdfsFS fs = getHdfsFS();
    int created = hdfsCreateDirectory(fs, path);
    if (created == -1) {
        fprintf(stderr, "Failed to create folder %s!\n", path);
    }
    hdfsDisconnect(fs);
    return created;
}

/**
 *  \brief check the status of a given directory
 *  \param [in] directory path
 *  \param [in] mode (0): READ, mode (1): WRITE
 *  \param [in] force: write by force,
 */

int dirCheck(const char* path, int mode = 0, bool force = true) /*mode: 0 read, 1 write*/
{
    const int READ = 0, WRITE = 1;

    hdfsFS fs = getHdfsFS();

    switch (mode) {
    case READ:
        if (hdfsExists(fs, path) != 0) {
            fprintf(stderr, "Input path \"%s\" does not exist!\n", path);
            hdfsDisconnect(fs);
            return -1;
        }
        break;
    case WRITE:
        if (hdfsExists(fs, path) == 0) {
            if (force) {
                if (hdfsDelete(fs, path) == -1) {
                    fprintf(stderr, "Error deleting %s!\n", path);
                    return -1;
                }
                int created = dirCreate(path);
                if (created == -1) {
                    return -1;
                }
            } else {
                fprintf(stderr, "Output path \"%s\" already exists!\n", path);
                hdfsDisconnect(fs);
                return -1;
            }
        } else {
            int created = dirCreate(path);
            if (created == -1) {
                fprintf(stderr, "Failed to create folder %s!\n", path);
                return -1;
            }
        }
        hdfsDisconnect(fs);
        break;
    }
    return 0;
}

/**
 *  \brief A buffered LineReader
 */

struct BufferedReader {

    char buf[HDFS_BUF_SIZE];

    hdfsFS fs;
    hdfsFile handle;
    tSize bufPos;
    tSize bufSize;

    std::string line;
    bool eof;

    BufferedReader(const char* path, hdfsFS fs)
    {
        this->fs = fs;
        this->handle = getRHandle(path, fs);
        this->bufPos = 0;
        this->bufSize = 0;

        this->line = (char*)malloc(LINE_DEFAULT_SIZE * sizeof(char));
        this->eof = false;
    }

    void readChunk()
    {
        bufSize = hdfsRead(fs, handle, buf, HDFS_BUF_SIZE);
        if (bufSize == -1) {
            fprintf(stderr, "Read Failure!\n");
            exit(-1);
        }
        if (bufSize == 0) {
            eof = true;
        }
        bufPos = 0;
    }

    char* getLine()
    {
        if (eof)
            return NULL;
        line.clear();
        char* pch;
        while ((pch = (char*)memchr(buf + bufPos, '\n', bufSize - bufPos))
               == NULL) {
            // flush buffer
            line.append(buf + bufPos, bufSize - bufPos);
            // read more data into buffer
            readChunk();
            if (eof)
                break;
        }
        if (eof && line.empty())
            return NULL;
        if (pch != NULL) {
            int length = pch - (buf + bufPos);
            line.append(buf + bufPos, length);
            bufPos += length + 1; //+1 to skip '\n'
        }
        return const_cast<char*>(line.c_str());
    }
};

/**
 *  \brief A buffered Writer
 */

struct BufferedWriter {
    hdfsFS fs;
    const char* path;
    int me; //-1 if there's no concept of machines (like: hadoop fs -put)
    int nxtPart;
    vector<char> buf;
    hdfsFile curHdl;

    BufferedWriter(const char* path, hdfsFS fs)
    {
        this->path = path;
        this->fs = fs;
        this->me = -1;
        this->curHdl = getWHandle(this->path, fs);
    }
    BufferedWriter(const char* path, hdfsFS fs, int me)
        : nxtPart(0)
    {
        this->path = path;
        this->fs = fs;
        this->me = me;
        curHdl = NULL;
        nextHdl();
    }

    ~BufferedWriter()
    {
        tSize numWritten = hdfsWrite(fs, curHdl, &buf[0], buf.size());
        if (numWritten == -1) {
            fprintf(stderr, "Failed to write file!\n");
            exit(-1);
        }
        buf.clear();

        if (hdfsFlush(fs, curHdl)) {
            fprintf(stderr, "Failed to 'flush' %s\n", path);
            exit(-1);
        }
        hdfsCloseFile(fs, curHdl);
    }

    //internal use only!
    void nextHdl()
    {
        //set fileName
        char fname[20];

        if (me >= 0) {
            sprintf(fname, "part_%d_%d", me, nxtPart);
        } else {
            sprintf(fname, "part_%d", nxtPart);
        }

        //flush old file
        if (nxtPart > 0) {
            if (hdfsFlush(fs, curHdl)) {
                fprintf(stderr, "Failed to 'flush' %s\n", path);
                exit(-1);
            }
            hdfsCloseFile(fs, curHdl);
        }
        //open new file
        nxtPart++;
        char* filePath = new char[strlen(path) + strlen(fname) + 2];
        sprintf(filePath, "%s/%s", path, fname);
        curHdl = getWHandle(filePath, fs);
        delete[] filePath;
    }

    void check()
    {
        if (buf.size() >= HDFS_BLOCK_SIZE) {
            tSize numWritten = hdfsWrite(fs, curHdl, &buf[0], buf.size());
            if (numWritten == -1) {
                fprintf(stderr, "Failed to write file!\n");
                exit(-1);
            }
            buf.clear();
            if (me != -1) // -1 means "output in the specified file only"
            {
                nextHdl();
            }
        }
    }

    void write(const char* content)
    {
        int len = strlen(content);
        buf.insert(buf.end(), content, content + len);
    }
};

/**
 *  \brief Put: local->HDFS
 *  \param [in] local directory path
 *  \param [in] hdfs directory path
 *  \param [in] force: write by force,
 */

void putf(char* localpath, char* hdfspath, bool force = true) //force put, overwrites target
{
    dirCheck(hdfspath, 1, force);
    hdfsFS fs = getHdfsFS();
    hdfsFS lfs = getlocalFS();

    BufferedReader* reader = new BufferedReader(localpath, lfs);
    BufferedWriter* writer = new BufferedWriter(hdfspath, fs, 0);

    const char* line = 0;

    while ((line = reader->getLine()) != NULL) {
        writer->check();
        writer->write(line);
        writer->write("\n");
    }

    delete reader;
    delete writer;

    hdfsDisconnect(lfs);
    hdfsDisconnect(fs);
}

//====== Dispatcher ======

struct sizedFName {
    char* fname;
    tOffset size;

    bool operator<(const sizedFName& o) const
    {
        return size > o.size; //large file goes first
    }
};

struct sizedFString {
    string fname;
    tOffset size;

    bool operator<(const sizedFString& o) const
    {
        return size > o.size; //large file goes first
    }
};

const char* rfind(const char* str, char delim)
{
    int len = strlen(str);
    int pos = 0;
    for (int i = len - 1; i >= 0; i--) {
        if (str[i] == delim) {
            pos = i;
            break;
        }
    }
    return str + pos;
}

vector<string>* dispatchRan(const char* inDir, int numSlaves) //remember to "delete[] assignment" after used
{ //locality is not considered for simplicity
    vector<string>* assignment = new vector<string>[numSlaves];
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[numSlaves];
    for (int i = 0; i < numSlaves; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
        }
    }
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < numSlaves; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    return assignment;
}

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<string>* dispatchLocality(const char* inDir, int numSlaves) //remember to "delete[] assignment" after used
{ //considers locality
    vector<string>* assignment = new vector<string>[numSlaves];
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[numSlaves];
    for (int i = 0; i < numSlaves; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    int avg = 0;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
            avg += fileinfo[i].mSize;
        }
    }
    avg /= numSlaves;
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    vector<sizedFName> recycler;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        istringstream ss(rfind(it->fname, '/'));
        string cur;
        getline(ss, cur, '_');
        getline(ss, cur, '_');
        int slaveOfFile = atoi(cur.c_str());
        if (assigned[slaveOfFile] + it->size <= avg) {
            assignment[slaveOfFile].push_back(it->fname);
            assigned[slaveOfFile] += it->size;
        } else
            recycler.push_back(*it);
    }
    for (it = recycler.begin(); it != recycler.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < numSlaves; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    return assignment;
}

vector<vector<string> >* dispatchRan(const char* inDir) //remember to delete assignment after used
{ //locality is not considered for simplicity
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(
        _num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
        }
    }
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    return assignmentPointer;
}

vector<vector<string> >* dispatchRan(vector<string> inDirs) //remember to delete assignment after used
{ //locality is not considered for simplicity
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(
        _num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    vector<sizedFString> sizedfile;
    for (int pos = 0; pos < inDirs.size(); pos++) {
        const char* inDir = inDirs[pos].c_str();
        int numFiles;
        hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
        if (fileinfo == NULL) {
            fprintf(stderr, "Failed to list directory %s!\n", inDir);
            exit(-1);
        }
        for (int i = 0; i < numFiles; i++) {
            if (fileinfo[i].mKind == kObjectKindFile) {
                sizedFString cur = { fileinfo[i].mName, fileinfo[i].mSize };
                sizedfile.push_back(cur);
            }
        }
        hdfsFreeFileInfo(fileinfo, numFiles);
    }
    //sort files by size
    sort(sizedfile.begin(), sizedfile.end());
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //allocate files to slaves
    vector<sizedFString>::iterator it;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    return assignmentPointer;
}

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<vector<string> >* dispatchLocality(const char* inDir) //remember to delete assignment after used
{ //considers locality
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(
        _num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    if (fileinfo == NULL) {
        fprintf(stderr, "Failed to list directory %s!\n", inDir);
        exit(-1);
    }
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //sort files by size
    vector<sizedFName> sizedfile;
    int avg = 0;
    for (int i = 0; i < numFiles; i++) {
        if (fileinfo[i].mKind == kObjectKindFile) {
            sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
            sizedfile.push_back(cur);
            avg += fileinfo[i].mSize;
        }
    }
    avg /= _num_workers;
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFName>::iterator it;
    vector<sizedFName> recycler;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        istringstream ss(rfind(it->fname, '/'));
        string cur;
        getline(ss, cur, '_');
        getline(ss, cur, '_');
        int slaveOfFile = atoi(cur.c_str());
        if (assigned[slaveOfFile] + it->size <= avg) {
            assignment[slaveOfFile].push_back(it->fname);
            assigned[slaveOfFile] += it->size;
        } else
            recycler.push_back(*it);
    }
    for (it = recycler.begin(); it != recycler.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    return assignmentPointer;
}

vector<vector<string> >* dispatchLocality(vector<string> inDirs) //remember to delete assignment after used
{ //considers locality
    vector<vector<string> >* assignmentPointer = new vector<vector<string> >(
        _num_workers);
    vector<vector<string> >& assignment = *assignmentPointer;
    hdfsFS fs = getHdfsFS();
    vector<sizedFString> sizedfile;
    int avg = 0;
    for (int pos = 0; pos < inDirs.size(); pos++) {
        const char* inDir = inDirs[pos].c_str();
        int numFiles;
        hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
        if (fileinfo == NULL) {
            fprintf(stderr, "Failed to list directory %s!\n", inDir);
            exit(-1);
        }
        for (int i = 0; i < numFiles; i++) {
            if (fileinfo[i].mKind == kObjectKindFile) {
                sizedFString cur = { fileinfo[i].mName, fileinfo[i].mSize };
                sizedfile.push_back(cur);
                avg += fileinfo[i].mSize;
            }
        }
        hdfsFreeFileInfo(fileinfo, numFiles);
    }
    tOffset* assigned = new tOffset[_num_workers];
    for (int i = 0; i < _num_workers; i++)
        assigned[i] = 0;
    //sort files by size
    avg /= _num_workers;
    sort(sizedfile.begin(), sizedfile.end());
    //allocate files to slaves
    vector<sizedFString>::iterator it;
    vector<sizedFString> recycler;
    for (it = sizedfile.begin(); it != sizedfile.end(); ++it) {
        istringstream ss(rfind(it->fname.c_str(), '/'));
        string cur;
        getline(ss, cur, '_');
        getline(ss, cur, '_');
        int slaveOfFile = atoi(cur.c_str());
        if (assigned[slaveOfFile] + it->size <= avg) {
            assignment[slaveOfFile].push_back(it->fname);
            assigned[slaveOfFile] += it->size;
        } else
            recycler.push_back(*it);
    }
    for (it = recycler.begin(); it != recycler.end(); ++it) {
        int min = 0;
        tOffset minSize = assigned[0];
        for (int j = 1; j < _num_workers; j++) {
            if (minSize > assigned[j]) {
                min = j;
                minSize = assigned[j];
            }
        }
        assignment[min].push_back(it->fname);
        assigned[min] += it->size;
    }
    delete[] assigned;
    return assignmentPointer;
}

void reportAssignment(vector<string>* assignment, int numSlaves)
{
    for (int i = 0; i < numSlaves; i++) {
        cout << "====== Rank " << i << " ======" << endl;
        vector<string>::iterator it;
        for (it = assignment[i].begin(); it != assignment[i].end(); ++it) {
            cout << *it << endl;
        }
    }
}

void reportAssignment(vector<vector<string> >* assignment)
{
    for (int i = 0; i < _num_workers; i++) {
        cout << "====== Rank " << i << " ======" << endl;
        vector<string>::iterator it;
        for (it = (*assignment)[i].begin(); it != (*assignment)[i].end();
             ++it) {
            cout << *it << endl;
        }
    }
}

#endif
