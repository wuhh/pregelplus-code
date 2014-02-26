CCOMPILE=mpic++
BLOGELLIB=/home/yanda/blogel/lib
PLATFORM=Linux-amd64-64
CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I $(BLOGELLIB)
LIB = -L$(HADOOP_HOME)/c++/$(PLATFORM)/lib 
LDFLAGS = -lhdfs -Wno-deprecated -O2

all: convert

convert: convert.cpp
	$(CCOMPILE) convert.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o convert

clean:
	-rm convert
