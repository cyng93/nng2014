#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>

#include "mpi.h"

#include "probsolver.h"
#include "options.h"


//#define debuging
#define enhance
#define MAX_RECV_WORKER     25
#define MAX_SEND_WORKER     1


#ifdef enhance
 #define TAG_WORK_ENQUIRE   0
 #define TAG_WORK_ASSIGN    1
 #define TAG_WORK_RETURN    2
 #define TAG_DONE           3

 #define TAG_WORK_ENQUIRE_OFFSET    0
 #define TAG_WORK_ASSIGN_OFFSET     1000
 #define TAG_WORK_RETURN_OFFSET     3000
 #define TAG_DONE_OFFSET            5000
#endif

using namespace std;

int size = 0, mpi_rank = 0;

int checkTag(int tag){
    switch(tag / 1000){
    case 0:
        return TAG_WORK_ENQUIRE;
    case 1:
        return TAG_WORK_ASSIGN;
    case 2:
        return TAG_WORK_ASSIGN;
    case 3:
        return TAG_WORK_RETURN;
    case 4:
        return TAG_WORK_RETURN;
    case 5:
        return TAG_DONE;
    case 6:
        return TAG_DONE;
    case 10:
        return TAG_WORK_ASSIGN; //9999 indicate no work to assign
    }
    return -1;
}


int main(int argc , char *argv[])
{
	MPI_Init(&argc ,&argv);
	double mpi_time = MPI_Wtime();

	Options option;
	if(!option.readOptions(argc, argv))
	{
		printf("\nAborted: Illegal Options.\n");
		return 0;
	}

	if(option.genLogFile())
	{
		printf("\nopen log(%s) and write info failed\n",option.logFileName);
		return 0;
	}

	clearFile(option.outputFileName);

	int *inputData;
	int probData[50*14];
	inputData = allocMem(1001*50*14);
	readFile(option.inputFileName,inputData);

	NonogramSolver nngSolver;
	nngSolver.setMethod(option.method);
	initialHash();

	/* MPI part */
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

	if( mpi_rank == 0 && size == 1 )
	{
		puts("SC mode");
		return 5;
	}

        int i;
        int done[size];
        int preDone[size];
	Board answer[1001];
        int doneFlag[1001];
	int probN;
        int tmpProbN;
        int lastProb;
        int tag;
        int recvProbN;
        int pendingAns;
        int pendingAssign;

        MPI_Status *status;
        MPI_Request *req;
        int *flag;
        Board *b;
        vector<MPI_Status *> send_status, recv_status;
        vector<MPI_Request *> send_req, recv_req;
        vector<int *> send_flag, recv_flag;
        vector<Board *> send_b, recv_b;


        memset(done, 0, size*sizeof(int));
        memset(doneFlag, 0, 1001*sizeof(int));

        probN = option.problemStart + mpi_rank;

        lastProb = probN;
        while(lastProb <= option.problemEnd)
            lastProb += size;
        lastProb -= size;

        pendingAns = 0;
        pendingAssign = 0;


	if( mpi_rank > size )
        {
	    fprintf(stderr,"Illegal MPI_Rank!\n");
            return 1;
        }

	while( 1 ) //while 1
	{
            if(recv_req.size() < MAX_RECV_WORKER){
                flag = new int;
                status = new MPI_Status;
                req = new MPI_Request;
                b = new Board;

                MPI_Irecv(b->data, 50, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, req);
                recv_req.push_back(req);
                recv_status.push_back(status);
                recv_flag.push_back(flag);
                recv_b.push_back(b);
            }

            for(i=0; i<(int)recv_req.size(); i++){
                MPI_Request_get_status(*(recv_req[i]), recv_flag[i], recv_status[i]);
                if(! *(recv_flag[i]) ) continue;

                req = recv_req[i];
                status = recv_status[i];
                flag = recv_flag[i];
                b = recv_b[i];

                tag = checkTag(status->MPI_TAG);
                //printf(" ID %d receive msg from %d, tag=%d\n", mpi_rank, status.MPI_SOURCE, tag);
                switch(tag){
                    case TAG_DONE:
                                if(done[status->MPI_TAG - TAG_DONE_OFFSET] == 0){
#ifdef debuging
                                    printf("\t\t%d recv %d done\n", mpi_rank, status->MPI_TAG-TAG_DONE_OFFSET);
#endif
                                    done[status->MPI_TAG - TAG_DONE_OFFSET] = 1;
                                    preDone[status->MPI_TAG - TAG_DONE_OFFSET] = 1;
                                }
                                break;
                    case TAG_WORK_ENQUIRE:
                                if(probN >= lastProb){ // left one problem, solve ourself
                                    MPI_Send(b->data, 50, MPI_UNSIGNED_LONG_LONG, status->MPI_SOURCE, 9999+TAG_WORK_ASSIGN_OFFSET, MPI_COMM_WORLD);
                                }
                                else{ // assign the lastProb to other who offer help
                                    MPI_Send(b->data, 50, MPI_UNSIGNED_LONG_LONG, status->MPI_SOURCE, lastProb+TAG_WORK_ASSIGN_OFFSET, MPI_COMM_WORLD);
#ifdef debuging
                                    printf("\t%d assign prob %d to %d\n", mpi_rank, lastProb, status->MPI_SOURCE);
#endif
                                    lastProb -= size;
                                    pendingAns++;
                                }
                                break;
                    case TAG_WORK_RETURN:
                                recvProbN = status->MPI_TAG-TAG_WORK_RETURN_OFFSET;
                                printf("\t%d recv %d's answer from %d\n", mpi_rank, recvProbN, status->MPI_SOURCE);
                                if(recvProbN >= 1 && recvProbN <= 1000){
                                    getData(inputData, recvProbN, probData);
                                    if( !checkAns(*b, probData) ){
                                        fprintf(stderr, "Fatal Error: Answer received is wrong\n");
                                    }
                                    answer[recvProbN] = *b;
                                    pendingAns--;
                                }
                                else{
                                    fprintf(stderr, "Illegal TAG = %d\n", status->MPI_TAG);
                                }
                                break;
                    default:
                                fprintf(stderr, "Fatal Error: received invalid tag %d in while1\n", status->MPI_TAG);
                                break;
                }


                recv_req.erase(recv_req.begin() + i);
                recv_status.erase(recv_status.begin() + i);
                recv_flag.erase(recv_flag.begin() + i);
                recv_b.erase(recv_b.begin() + i);
                delete(req);
                delete(status);
                delete(flag);
                delete(b);

            }

	    if( probN > lastProb){
                if(pendingAns != 0){
                    //printf(" \t%d pending %d more answer..\n", mpi_rank, pendingAns);
                    continue;
                }


                // inform other node
                for(i=0; i<size; i++){
                    if(i == mpi_rank)
                        done[i] = 1;
                    else
                        MPI_Send(b->data, 50, MPI_UNSIGNED_LONG_LONG, i, mpi_rank+TAG_DONE_OFFSET, MPI_COMM_WORLD);
                }

#ifdef debuging
                printf(" %d finish its probs\n", mpi_rank);
#endif
                break;
            }


		printf("ID:%d obtain problem %d / %d\n",mpi_rank,probN,lastProb);
                getData(inputData, probN, probData);

		if( !nngSolver.doSolve(probData) )
			return 1;

		Board ans = nngSolver.getSolvedBoard();

		if( option.selfCheck && !checkAns(ans, probData) )
		{
			printf("ID:%d Fatal Error: Answer not correct\n",mpi_rank);
			return 1;
		}
		answer[probN] = ans;
                doneFlag[probN] = 1;

                probN+=size;
	}
#ifdef enhance
        while(1) //while2
        {
            if(recv_req.size() < MAX_RECV_WORKER){
                flag = new int;
                status = new MPI_Status;
                req = new MPI_Request;
                b = new Board;

                MPI_Irecv(b->data, 50, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, req);
                recv_req.push_back(req);
                recv_status.push_back(status);
                recv_flag.push_back(flag);
                recv_b.push_back(b);
            }

            for(i=0; i<(int)recv_req.size(); i++){
                MPI_Request_get_status(*recv_req[i], recv_flag[i], recv_status[i]);
                if(! *(recv_flag[i]) ) continue;

                req = recv_req[i];
                status = recv_status[i];
                flag = recv_flag[i];
                b = recv_b[i];

                tag = checkTag(status->MPI_TAG);
                switch(tag){
                    case TAG_DONE:
                                if(done[status->MPI_TAG - TAG_DONE_OFFSET] == 0){
#ifdef debuging
                                    printf("\t\t%d receive %d done\n", mpi_rank, status->MPI_TAG-TAG_DONE_OFFSET);
#endif
                                    done[status->MPI_TAG - TAG_DONE_OFFSET] = 1;
                                    preDone[status->MPI_TAG - TAG_DONE_OFFSET] = 1;
                                }
                                break;
                    case TAG_WORK_ENQUIRE:
                                MPI_Send(b->data, 50, MPI_UNSIGNED_LONG_LONG, status->MPI_SOURCE, 9999+TAG_WORK_ASSIGN_OFFSET, MPI_COMM_WORLD);
                                break;
                    case TAG_WORK_ASSIGN:
                                tmpProbN = status->MPI_TAG-TAG_WORK_ASSIGN_OFFSET;
#ifdef debuging
                                printf("\t\t[w2] %d recv assignment %d(%d) from %d\n", mpi_rank, tmpProbN, (tmpProbN>=1&&tmpProbN<=1000), status->MPI_SOURCE);
#endif
                                if(tmpProbN == 9999)
                                    preDone[status->MPI_SOURCE] = 1;
                                else if(tmpProbN >=1 && tmpProbN <= 1000){
                                    getData(inputData, tmpProbN, probData);

	                            if( !nngSolver.doSolve(probData) )
	                                return 1;

	                            Board ans = nngSolver.getSolvedBoard();

	                            if( option.selfCheck && !checkAns(ans, probData) )
	                            {
	                                printf("ID:%d Fatal Error: Answer not correct\n", mpi_rank);
	                                return 1;
	                            }

                                    answer[tmpProbN] = ans;
                                    doneFlag[tmpProbN] = 1;
                                    MPI_Send(ans.data, 50, MPI_UNSIGNED_LONG_LONG, status->MPI_SOURCE, tmpProbN+TAG_WORK_RETURN_OFFSET, MPI_COMM_WORLD);
#ifdef debuging
                                    printf("\t%d send %d's ans to %d complete\n", mpi_rank, tmpProbN, status->MPI_SOURCE);
#endif
                                }
                                pendingAssign = 0;
                                break;
                    case TAG_WORK_RETURN:
                                recvProbN = status->MPI_TAG - TAG_WORK_RETURN_OFFSET;
#ifdef debuging
                                printf("recv %d's answer from %d\n", recvProbN, status->MPI_SOURCE);
#endif
                                if(recvProbN >= 1 && recvProbN <= 1000){
                                    getData(inputData, recvProbN, probData);
                                    if( !checkAns(*b, probData) ){
                                        fprintf(stderr, "Fatal Error: Answer received is wrong\n");
                                    }
                                    answer[recvProbN] = *b;
                                    pendingAns--;
                                }
                                else{
                                    fprintf(stderr, "Illegal TAG = %d\n", status->MPI_TAG);
                                }
                                break;
                    default:
                            fprintf(stderr,"Invalid Tag: %dreceive TAG %d in while2\n", mpi_rank, status->MPI_TAG);
                            break;
                }

                recv_req.erase(recv_req.begin() + i);
                recv_status.erase(recv_status.begin() + i);
                recv_flag.erase(recv_flag.begin() + i);
                recv_b.erase(recv_b.begin() + i);
                delete(req);
                delete(status);
                delete(flag);
                delete(b);
            }

            int allDone = 1;
            for(i=0; i<size; i++){
                if(!done[i]){
                    allDone = 0;
                    break;
                }
            }
            if(allDone) break;


            // ask for problem from other node
            if (pendingAssign == 1) continue;

            for(i=mpi_rank+1; ; i++){
                i %= size;
                if(i==mpi_rank) break;
                else if(pendingAssign==1 || preDone[i] == 1) continue;

                MPI_Send(b->data, 50, MPI_UNSIGNED_LONG_LONG, i, TAG_WORK_ENQUIRE, MPI_COMM_WORLD);
#ifdef debuging
                printf("\t%d enquire prob from %d\n", mpi_rank, i);
#endif
                pendingAssign = 1;

            }

        }
#endif


	printf("ID:%d Run completed, Wait barrier, Time:%lf\n",mpi_rank,MPI_Wtime()-mpi_time);
	MPI_Barrier(MPI_COMM_WORLD);

        // centralize all answer to node 0
        int cnt = 0;
        if(mpi_rank == 0){

            for(i=option.problemStart; i<=option.problemEnd; i++)
                cnt = (doneFlag[i]==1) ? (cnt+1) : cnt;

            while(1)
            {
                if(cnt == (option.problemEnd-option.problemStart+1))
                    break;

                if(recv_req.size() < MAX_RECV_WORKER){
                    flag = new int;
                    status = new MPI_Status;
                    req = new MPI_Request;
                    b = new Board;

                    MPI_Irecv(b->data, 50, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, req);
                    recv_req.push_back(req);
                    recv_status.push_back(status);
                    recv_flag.push_back(flag);
                    recv_b.push_back(b);
                }

                for(i=0; i<(int)recv_req.size(); i++){
                    MPI_Request_get_status(*recv_req[i], recv_flag[i], recv_status[i]);
                    if(! *(recv_flag[i]) ) continue;

                    req = recv_req[i];
                    status = recv_status[i];
                    flag = recv_flag[i];
                    b = recv_b[i];

                    tag = checkTag(status->MPI_TAG);

                    if(tag == TAG_WORK_RETURN){
                        recvProbN = status->MPI_TAG - TAG_WORK_RETURN_OFFSET;

                        if(recvProbN >=1 && recvProbN <= 1000){
                            getData(inputData, recvProbN, probData);
                            if( !checkAns(*b, probData) ){
                                fprintf(stderr, "Fatal Error: received answer(%d) is wrong\n", i);
                            }
                            answer[recvProbN] = *b;
                            cnt++;
                        }

                    }
                    else{
                        fprintf(stderr, "Fatal Error: %d recv invalid tag %d\n", mpi_rank, status->MPI_TAG);
                    }

                    recv_req.erase(recv_req.begin() + i);
                    recv_status.erase(recv_status.begin() + i);
                    recv_flag.erase(recv_flag.begin() + i);
                    recv_b.erase(recv_b.begin() + i);
                    delete(req);
                    delete(status);
                    delete(flag);
                    delete(b);
                }

            }
        }
        else{
            for(i=option.problemStart; i<=option.problemEnd; i++){
                if(doneFlag[i] == 1){
                    MPI_Send(answer[i].data, 50, MPI_UNSIGNED_LONG_LONG, 0, i+TAG_WORK_RETURN_OFFSET, MPI_COMM_WORLD);
                }
            }
        }


        if( mpi_rank == 0 )
	{
		for( int i = 0; i < option.problemEnd-option.problemStart+1 ; i++ )
                        if(i % (mpi_rank+1) == 0)
    			    printBoard(option.outputFileName, answer[option.problemStart+i], option.problemStart+i);

		printf("ID:%d All completed, Wait barrier, Time:%lf\n",mpi_rank,MPI_Wtime()-mpi_time);
	}

	delete[] inputData;
	MPI_Finalize();
	return 0;
}


