//============================================================================
// Author      : Atakan Arıkan / CmpE - 2011400243
//============================================================================

#include <iostream>
#include <mpi.h>
#include <fstream>
#include <string>
#include <vector>
#include <iterator>
#include <sstream>
#include <stdlib.h>
#include <string.h>
using namespace std;

int main(int argc, char *argv[]){
	int size, rank, index, arrSize, beginIndex, endIndex, movement, deepSleepTime, currentTime, lastLight, lastAwake, lastTempAwake, state, awakeNum;
	double diffX, diffY, diffZ;
	int startH, startM, startSec;
	/*
	 * state = 0 -> deep sleep
	 * state = 1 -> light sleep
	 * state = 2 -> awake
	 */
	double **sleepData;
	int *awakeData;
	string startingHour;
	char *filename;
	int movements, tempmovements;
	int deepSleepCounts, tempdeepSleepCounts;
	int awakeNums, tempawakeNums;
	/* initialize Message Passing Interface(MPI) */
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	/* master node */
	if(rank==0){
		startingHour=argv[argc-1]; // starting time of the sleeping data
		filename=argv[argc-2]; // get the filename
		/* extract the start hour data */
		size_t pos = 0;
		string token, startHour, startMin;
		string delimiter = ":";
		while ((pos = startingHour.find(delimiter)) != string::npos) {
			token = startingHour.substr(0, pos);
			startHour = token;
			startH = atoi(token.c_str());
			startingHour.erase(0, pos + delimiter.length());
		}
		startMin = startingHour;
		startM = atoi(startingHour.c_str());
		startSec = startH*3600 + startM*60; // find the value of starting hour in terms of seconds.
		index = -1;
		arrSize = -1;
		movement = 0;
		int temp=0;
		int totalMovs=0;
		/*get the size of the data*/
		ifstream myfiletemp(filename);
		string line;
		if(myfiletemp.is_open()){
			while(getline(myfiletemp, line)){
				arrSize++;
			}
		}else{
			/* unsuccessful */
			cerr << "Unable to open file!" << endl;
			return 0;
		}
		myfiletemp.close();
		/* initialize our array */
		sleepData = new double*[arrSize];
		awakeData = new int[arrSize];
		for(int i=0; i<arrSize; i++){
			sleepData[i]= new double[4];
		}
		ifstream myfile(filename);
		if(myfile.is_open()){
			while(getline(myfile, line)){
				if(index==-1) index++;  //first line
				else{
					/* split the string*/
					istringstream iss(line);
					vector<string> tokens;
					copy(istream_iterator<string>(iss),
							istream_iterator<string>(),
							back_inserter(tokens));
					/* insert the tokens to the array one by one. */
					for(int j=0; j<tokens.size(); j++){
						sleepData[index][j]=atof(tokens[j].c_str());
					}
					index++;
				}
			}
		}else{
			/* unsuccessful */
			cerr << "Unable to open file!" << endl;
			return 0;
		}
		myfile.close();
		movements=0;
		deepSleepCounts = 0;
		awakeNums=0;
		for(int i=1;i<size;i++){
			beginIndex = (i-1)*(arrSize/(size-1));
			endIndex = i*(arrSize/(size-1));
			if(i==size-1){ //data for the last processor
				endIndex = arrSize - 1;
			}
			int tempSize = endIndex-beginIndex;
			currentTime = beginIndex;
			lastLight = -480;
			lastAwake = -480;
			lastTempAwake = -480;
			awakeNum=0;
			diffX=sleepData[beginIndex][1]-sleepData[beginIndex+1][1];
			diffY=sleepData[beginIndex][2]-sleepData[beginIndex+1][2];
			diffZ=sleepData[beginIndex][3]-sleepData[beginIndex+1][3];
			if((diffX <= 0.003 && diffX >= -0.003) || (diffY <= 0.003 && diffY >= -0.003) || (diffZ <= 0.003 && diffZ >= -0.003)){
				state=0;
			}else if((diffX < 0.008 && diffX > -0.008) || (diffY < 0.008 && diffY > -0.008) || (diffZ < 0.008 && diffZ > -0.008)){
				state=1;
			}else{
				state=2;
			}
			deepSleepTime=0;
			/* start sending necessarry data to the worker nodes */
			MPI_Send(&tempSize,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&deepSleepTime,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&movement,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&currentTime,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&lastLight,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&lastAwake,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&lastTempAwake,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&state,1,MPI_INT,i,0,MPI_COMM_WORLD);
			MPI_Send(&diffX,1,MPI_DOUBLE,i,0,MPI_COMM_WORLD);
			MPI_Send(&diffY,1,MPI_DOUBLE,i,0,MPI_COMM_WORLD);
			MPI_Send(&diffZ,1,MPI_DOUBLE,i,0,MPI_COMM_WORLD);
			MPI_Send(&awakeNum,1,MPI_INT,i,0,MPI_COMM_WORLD);
			/* send the sleep data to worker processors */
			for(int m=beginIndex; m<=endIndex; m++){ // m<=endIndex since we want the endIndex line to be in 2 processors.
				for(int j=0; j<4; j++){
					MPI_Send(&sleepData[m][j],1,MPI_DOUBLE,i,0,MPI_COMM_WORLD);
				}
			}
			/* receive the finished data from worker nodes */
			MPI_Recv(&tempmovements,1,MPI_INT,i,0,MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			movements+=tempmovements;
			MPI_Recv(&tempdeepSleepCounts,1,MPI_INT,i,0,MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			deepSleepCounts+=tempdeepSleepCounts;
			MPI_Recv(&tempawakeNums,1,MPI_INT,i,0,MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			for(int m=0; m<tempawakeNums; m++){
				MPI_Recv(&awakeData[m+awakeNums],1,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			}
			awakeNums+=tempawakeNums;
		}
		/* let's start writing to our output file. */
		string outputName = "output" + startHour + startMin + ".txt";
		char *outFile=new char[outputName.size()+1];
		outFile[outputName.size()]=0;
		memcpy(outFile,outputName.c_str(),outputName.size());
		ofstream off;
		off.open(outFile);
		off << "Number of moves:\n";
		off << movements << "\n";
		off << "Total deep sleep:\n";
		off << deepSleepCounts/3600 << ":" << (deepSleepCounts%3600)/60 << "\n";
		off << "Awake at:";
		for(int i=0; i<awakeNums; i++){
			off << "\n" <<(startSec+awakeData[i])/3600 << ":" << ((startSec+awakeData[i])%3600)/60;
		}
		off.close();
	}
	/* worker nodes */
	else {
		/*receive the data from master node */
		MPI_Recv(&arrSize,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&deepSleepTime,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&movement,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&currentTime,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&lastLight,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&lastAwake,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&lastTempAwake,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&state,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&diffX,1,MPI_DOUBLE,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&diffY,1,MPI_DOUBLE,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&diffZ,1,MPI_DOUBLE,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		MPI_Recv(&awakeNum,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		/* create our 2D array */
		sleepData = new double*[arrSize+1];
		awakeData = new int[arrSize];
		for(int i=0; i<=arrSize; i++){
			sleepData[i] = new double[4]; // 4 is the data count: time, accX, accY, accZ
		}
		/* receive the partial sleep data from the master processor */
		for(int m=0; m<=arrSize; m++){
			for(int j=0; j<4; j++){
				MPI_Recv(&sleepData[m][j],1,MPI_DOUBLE,0,0,MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			}
		}
		for(int i=0; i<arrSize; i++){ // where the actual work gets done.
			currentTime=sleepData[i][0];
			diffX=sleepData[i][1]-sleepData[i+1][1]; // difference in X-axis
			diffY=sleepData[i][2]-sleepData[i+1][2]; // difference in Y-axis
			diffZ=sleepData[i][3]-sleepData[i+1][3]; // difference in Z-axis
			if((diffX >= 0.03 || diffX <= -0.03) || (diffY >= 0.03 || diffY <= -0.03) || (diffZ >= 0.03 || diffZ <= -0.03)){
				/* we are awake */
				if(currentTime>(lastTempAwake+480)){
					lastTempAwake=currentTime;
					awakeData[awakeNum]=currentTime+1;
					awakeNum++;
				}
				if(currentTime>(lastLight+480) && currentTime>(lastAwake+480) && state < 2){
					movement++;
					lastAwake=currentTime;
					lastTempAwake=currentTime;
					lastLight=currentTime;
					state = 2;
				}
			}else if((diffX > 0.008 || diffX < -0.008) || (diffY > 0.008 || diffY < -0.008) || (diffZ > 0.008 || diffZ < -0.008)){
				/* we are in light sleep */
				if(currentTime>(lastLight+480)&& state==0){ // 8 minute rule, and we only count a move if we were in deep sleep.
					movement++;
					state = 1;
					lastLight=currentTime;
				}
			}else if((diffX <= 0.008 && diffX >= -0.008) || (diffY <= 0.008 && diffY >= -0.008) || (diffZ <= 0.008 && diffZ >= -0.008)){
				/* we are in deep sleep */
				deepSleepTime++;
				state = 0;
			}
		}
		/* send finished data back to the master node */
		MPI_Send(&movement,1,MPI_INT,0,0,MPI_COMM_WORLD);
		MPI_Send(&deepSleepTime,1,MPI_INT,0,0,MPI_COMM_WORLD);
		MPI_Send(&awakeNum,1,MPI_INT,0,0,MPI_COMM_WORLD);
		for(int j=0; j<awakeNum; j++){
			MPI_Send(&awakeData[j],1,MPI_INT,0,0,MPI_COMM_WORLD);
		}
	}
	MPI_Finalize();
	return(0);
	/* end of the program */
}
