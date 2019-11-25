#include <stdio.h>
#include "simplebully.h"



int MAX_ROUNDS = 1;           // number of rounds to run the algorithm
double TX_PROB = 1.0 - ERROR_PROB;    // probability of transmitting a packet successfully

unsigned long int get_PRNG_seed() {
  struct timeval tv;
  gettimeofday(&tv,NULL);
  unsigned long time_in_micros = 1000000 * tv.tv_sec + tv.tv_usec + getpid();//find the microseconds for seeding srand()

  return time_in_micros;
}        

bool is_timeout(time_t start_time) {
  // YOUR CODE GOES HERE
}


bool will_transmit() {
  // YOUR CODE GOES HERE
  
}


bool try_leader_elect() {
  // first toss a coin: if prob > 0.5 then attempt to elect self as leader
  // Otherwise, just keep listening for any message
  double prob = rand() / (double) RAND_MAX;       // number between [0.0, 1.0]
  bool leader_elect = (prob > THRESHOLD);
  
  return leader_elect;
}


int main(int argc, char *argv[]) {

  int myrank, np;
  int current_leader = 0;               // default initial leader node
  int opt; // used to parse the command line options

  // YOUR CODE FOR MPI Initiliazation GOES HERE
  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &np);
  MPI_Comm_rank(comm, &myrank);

  // determine command line options
  while ((opt=getopt(argc,argv,"c:m:t:"))!= EOF) {
    switch (opt) {
      case 'c':
        // user input argv[1]: designated initial leader 
        current_leader = atoi(optarg);
        if(!(current_leader > 0) || (current_leader >= np)) {
          usage();
        }
        break;
      case 'm':
        // user input argv[2]: how many rounds to run the algorithm
        MAX_ROUNDS = atoi(optarg);
        if(!(MAX_ROUNDS > 0)) {
          usage();
        }
        break;
      case 't':
        // user input argv[3]: packet trasnmission success/failure probability
        TX_PROB = atof(optarg);
        if((0.0 >= TX_PROB) || (1.0 <= TX_PROB)) {
          usage();
        }
        break;
      case '?':
        usage();
        break;
      default:
        usage();
    }
  }
  
  if(myrank == current_leader) {
    printf("\n*******************************************************************");
    printf("\n*******************************************************************");
    printf("\n Initialization parameters:: \n\tMAX_ROUNDS = %d \n\tinitial leader = %d \n\tTX_PROB = %f\n", MAX_ROUNDS, current_leader, TX_PROB);
    printf("\n*******************************************************************");
    printf("\n*******************************************************************\n\n");
  }

  // srand(get_PRNG_seed());   // HINT: COMMENT THIS LINE UNTIL YOU ARE SURE THAT YOUR CODE IS CORRECT. THIS WILL AID IN THE DEBUGGING PROCESS
  
  MPI_Status status;
  MPI_Request request;
  int mytoken = -1;
  // YOUR CODE FOR SETTING UP succ and pred GOES HERE
  int succ = (myrank + 1) % np; // succ = successor on ring;
  int pred = (myrank + np - 1) % np; // pred = predecessor on ring
  int round;


  for (round = 0; round < MAX_ROUNDS; round++) {
    if (myrank == current_leader) {
      printf("\n*********************************** ROUND %d ******************************************\n", round);
      if (try_leader_elect()) {
        mytoken = generate_token();
        printf("\n[rank %d][%d] New leader started\ttoken: %d\n", myrank, round, mytoken);
        fflush(stdout);
        msg[0] = myrank;
        msg[1] = mytoken;
        // then send a leader election message to next node on ring, after
        // generating a random token number. Largest token among all nodes will win.
        MPI_Send(&msg, 2, MPI_INT, succ, LEADER_ELECTION_MSG_TAG, comm);
        printf("\n[rank %d][%d] SENT LEADER ELECTION MSG to node %d with TOKEN = %d, tag = %d\n", myrank, round, succ, mytoken, LEADER_ELECTION_MSG_TAG);
        fflush(stdout);
      } else {
        // Otherwise, send a periodic HELLO message around the ring
      
        printf("\n[rank %d][%d] SENT HELLO MSG to node %d with TOKEN = %d, tag = %d\n", myrank, round, succ, mytoken, HELLO_MSG_TAG);
        fflush(stdout);
      }
    
      // Now FIRST issue a speculative MPI_IRecv() to receive data back
      int recv_buf[2];
      bool flag = false;    // Will keep track of whether a message was received before time out
      // YOUR CODE GOES HERE  
    
      // Next, you need to check if time out has occured. If time out, then you need to cancel the earlier issued speculative MPI_Irecv. 
      // YOUR CODE GOES HERE 
      
      // We receive the message from predecessor node and decide appropriate action based on the message TAG
      // YOUR CODE GOES HERE
      if (flag) {
        // If HELLO MSG received, do nothing
        // If LEADER ELECTION message, then determine who is the new leader and send out a new leader notification message
        switch (status.MPI_TAG) {
          case HELLO_MSG_TAG:
            printf("\n[rank %d][%d] HELLO MESSAGE completed ring traversal!\n", myrank, round);
            fflush(stdout);
            break;
          case LEADER_ELECTION_MSG_TAG:
            // Send a new leader message
            printf("\n[rank %d][%d] NEW LEADER FOUND! new leader = %d, with token = %d\n", myrank, round, current_leader, recv_buf[1]);
            fflush(stdout);
            break;
          default: ;  // do nothing
        }
      }
    } else {
      // Wait for a message to arrive until time out occurs
      // YOUR CODE GOES HERE 

      if ( ) {
        // You want to first receive the message so as to remove it from the MPI Buffer 
        // Then determine action depending on the message Tag field
        // YOUR CODE GOES HERE

        if (status.MPI_TAG == HELLO_MSG_TAG) {
          // Forward the message to next node
          printf("\n\t[rank %d][%d] Received and Forwarded HELLO MSG to next node = %d\n", myrank, round, succ);
          fflush(stdout);
        } else if (status.MPI_TAG == LEADER_ELECTION_MSG_TAG) {
          // Fist probabilistically see if wants to become a leader.
          // If yes, then generate own token and test if can become leader.
          // If can become leader, then update the LEADER ELECTION Message appropriately and retransmit to next node
          // Otherwise, just forward the original received LEADER ELECTION Message
          // With a probability 'p', forward the message to next node
          // This simulates link or node failure in a distributed system
          if ( ) {
            printf("\n\t[rank %d][%d] My new TOKEN = %d\n", myrank, round, mytoken);
            fflush(stdout);
          } else {
            printf("\n\t[rank %d][%d] Will not participate in Leader Election.\n", myrank, round);
            fflush(stdout);
          }

          // Forward the LEADER ELECTION Message

          // Finally, wait to hear from current leader who will be the next leader
          printf("\n\t[rank %d][%d] NEW LEADER :: node %d with TOKEN = %d\n", myrank, round, current_leader, recv_buf[1]);
          fflush(stdout);
          
          // Forward the LEADER ELECTION RESULT MESSAGE
        }   
      } 
    }
    // Finally hit barrier for synchronization of all nodes before starting new round of message sending
  }
  printf("\n** Leader for NODE %d = %d\n", myrank, current_leader);

  // Finalize MPI for clean exit
  return 0;
}
