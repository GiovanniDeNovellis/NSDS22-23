#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include "helpers.c"
#define MINUTE 60
#define HOUR (60*MINUTE)
#define DAY (24*HOUR)
#define MONTH (31*DAY)
#define MAIN_PROCESS_RANK 0
//Input parameters
int num_individuals=300, inf_individuals=50, world_w=1000, world_l=1000, country_w=500, country_l=500,
spreading_distance=70, t_step=300, t_simulated=11*DAY;
//THESE PARAMETERS REPRESENT A SMALL WORLD COMPARED TO THE NUMBER OF INDIVIDUALS, SO A BIG PART OF THE INDIVIDUALS WILL BE INFECTED WITHIN THE FIRST DAY 
//TO SEE THE EVOLUTION OF THE POPULATION, YOU CAN SET t_simulated TO:
//----- 1*DAY: AT THE FIRST TIME STEP SUMMARY THERE WILL BE ONLY THE INITIAL INFECTED INDIVIDUALS AS INFECTED AND SOME EXPOSED,
//  AT THE SECOND SUMMARY (END OF FIRST DAY) A BIG PART OF THE POPULATION IS INFECTED
//----- 10*DAY: THE INITIAL INFECTED INDIVIDUALS (50) ARE NOW IMMUNE AND THE OTHERS WILL BE IMMUNE BY THE END OF THE FOLLOWING DAY
//----- 11*DAY: ALL THE INFECTED INDIVIDUALS ARE NOW IMMUNE
//----- 3*MONTH + 10*DAY: THE INITIAL INFECTED INDIVIDUALS ARE NOW SUSCEPTIBLE AGAIN
//----- 3*MONTH + 11*DAY: ALL THE POPULATION IS NOW SUSCEPTIBLE (THE INFECTION ENDED)
// TO RUN A CASE WHERE THE WHOLE POPULATION GETS INFECTED CHANGE THE SPREADING DISTANCE FROM 20 TO 100
double velocity=1.4;

//Function declarations
void print_input_params();
country_type create_country(int rank);
int calc_num_countries(int world_w, int world_l, int country_w, int country_l);

int main(int argc, char** argv){
    int rank, size;
    MPI_Init(NULL, NULL);
    int day_end_timestamp=DAY;

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == MAIN_PROCESS_RANK) {
        print_input_params(size);
    }
    int num_countries=calc_num_countries(world_w, world_l, country_w, country_l);
    country_type countries[num_countries];
    //EACH PROCESS CREATES THE LIST OF COUNTRIES;
    for(int i=0; i<num_countries; i++){
    	countries[i]=create_country(i);
    }
    //UNCOMMENT THE FOLLOWING TO DEBUG COUNTRY INITIALIZATION
    
    if(rank==MAIN_PROCESS_RANK){
    	for(int i=0; i<num_countries; i++){
    		printf("Country %d, x_start %d, x_end %d, y_start %d, y_end %d \n", countries[i].country_id, countries[i].x_start, countries[i].x_end, countries[i].y_start, countries[i].y_end);
    	}
    }
    
    //SPLITTING THE INDIVIDUALS EVENLY BETWEEN PROCESSES
    int process_individuals=num_individuals/size;
    int process_infected= inf_individuals/size;
    MPI_Barrier(MPI_COMM_WORLD);
    //Adjusting the split to account for all the individuals
    if(rank == MAIN_PROCESS_RANK) {
    	if(num_individuals%size!=0){
    		process_individuals+=(num_individuals%size);
    	}
    	if(inf_individuals%size!=0){
    		process_infected+=(inf_individuals%size);
    	}
    }
    printf("Process %d, individuals: %d, infected: %d \n", rank, process_individuals, process_infected);
    //Initializing the individuals of each process
    individual_type * individuals;
    if(rank == MAIN_PROCESS_RANK)
    	individuals = initialize_individuals(process_individuals, process_infected, countries, num_countries, 0, rank);
    else 
    	individuals = initialize_individuals(process_individuals, process_infected, countries, num_countries, num_individuals%size, rank);
    MPI_Barrier(MPI_COMM_WORLD);
    //UNCOMMENT THE FOLLOWING TO DEBUG INDIVIDUALS INITIALIZATION
    /*
    for(int i=0; i<process_individuals; i++){
    	printf("Process %d, Id %d, Country %d, x_pos: %d, y_pos: %d, time: %d, status: %c , direction: %c\n", rank, individuals[i].id, individuals[i].country, individuals[i].x_pos, individuals[i].y_pos, individuals[i].time_status, individuals[i].status,  individuals[i].direction);
    }
    */
    individual_type * all_individuals;
    MPI_Datatype INDIVIDUAL;
    defineStructIndividual(&INDIVIDUAL);
    MPI_Datatype SUMMARY;
    defineStructSummary(&SUMMARY);
    //--------------------------------------------------MAIN LOOP-----------------------------------------------------------------
    for(int t=0; t<=t_simulated; t+=t_step){
	for(int i = 0; i<process_individuals; i++){
	  //COMPUTING NEW POSITION AND DIRECTION OF EACH INDIVIDUAL	
	  individuals[i] = update_position(individuals[i], countries, world_w, world_l, country_w, velocity, t_step);
	  }
	MPI_Barrier(MPI_COMM_WORLD);
	//NOW NEED TO GATHER EVERY INDIVIDUAL TO EVERY PROCESS FOR NEIGHBOURS PROCESSING USING ALLGATHERV
	//FIRST, EVERY PROCESS NEEDS TO HAVE THE ARRAY WITH THE NUMBER OF INDIVIDUALS COMING FROM EACH PROCESS
	int recvcounts[size];
	int displs[size];
	int numIndividualsPerProcess[size];
	for(int i=0; i<size; i++){
		recvcounts[i]=1;
		displs[i]=i;
	}
	MPI_Allgatherv(&process_individuals, 1, MPI_INT, numIndividualsPerProcess, recvcounts, displs, MPI_INT, MPI_COMM_WORLD);
	//NOW I CAN GATHER ALL THE INDIVIDUALS TO EVERY PROCESS AND PROCEED WITH THE NEIGHBOUR PROCESSING
	individual_type allIndividuals[num_individuals]; //Array where all the individuals will be stored
	//Computing the new displacements for the second gatherv
	for(int i=1; i<size; i++)
		displs[i]=displs[i-1]+numIndividualsPerProcess[i-1];
	MPI_Allgatherv(individuals, process_individuals, INDIVIDUAL, allIndividuals, numIndividualsPerProcess, displs, INDIVIDUAL, MPI_COMM_WORLD);
	//NOW ALL THE INDIVIDUALS ARE GATHERED INTO THE allIndividuas ARRAY
	//UPDATING THE PROCESS'S INDIVIDUALS STATUS
	for(int i=0; i<process_individuals; i++){
		individuals[i]=update_status_individual(individuals[i], allIndividuals, num_individuals, spreading_distance);
	}
	//THE FOLLOWING CAN BE UNCOMMENTED FOR DETAILED INDIVIDUALS DEBUGGING 
	/*
	for(int i=0; i<process_individuals; i++){
		printf("Process %d, Time: %d Id %d, Country %d, x_pos: %d, y_pos: %d, time: %d, status: %c , direction: %c\n", rank, t, individuals[i].id, individuals[i].country, individuals[i].x_pos, individuals[i].y_pos, individuals[i].time_status, individuals[i].status,  individuals[i].direction);
	}
	*/
	MPI_Barrier(MPI_COMM_WORLD);
	if(t>=day_end_timestamp || t==0){
	//USE if(1) FOR SUMMARY AT EACH TIME STEP
	//if(1){
		//I AM AT THE END OF A DAY, NEED TO COMPUTE SUMMARY DATA AND SEND TO MAIN PROCESS
		if(t!=0)
			day_end_timestamp+=DAY;
		country_summary * summaries;
		//Every proces creates it's array with a struct for each country present in the program and the number of infected, exposed etc... from it's individuals and sends them to the main process 
		summaries = compute_country_summaries(individuals, num_countries, process_individuals);
		//CAN NOW GATHER THE SUMMARIES TO THE MAIN PROCESS THAT WILL PRINT THE FINAL STATISTICS
		country_summary * all_summaries;
		if(rank==MAIN_PROCESS_RANK)
			all_summaries = malloc(sizeof(country_summary)*num_countries*size);
		MPI_Gather(summaries, num_countries, SUMMARY, all_summaries, num_countries, SUMMARY, MAIN_PROCESS_RANK, MPI_COMM_WORLD);
		
		if(rank==MAIN_PROCESS_RANK){
			for(int i=0; i<num_countries; i++){
			  	int num_infected=get_statistic_summaries(all_summaries, i, num_countries*size, 'I');
			  	int num_exposed=get_statistic_summaries(all_summaries, i, num_countries*size, 'E');
			  	int num_susceptibles=get_statistic_summaries(all_summaries, i, num_countries*size, 'S');
			  	int num_immune=get_statistic_summaries(all_summaries, i, num_countries*size, 'X');
			  	printf("Process: %d, Time: %d, Country: %d, Susceptibles: %d, Exposed: %d, Infected: %d, Immune: %d \n", rank, t+t_step, i, num_susceptibles, num_exposed, num_infected, num_immune);
			}
		}
	}
    }
    MPI_Finalize();
    return 0;
}

//PRINTS THE INPUT PARAMETERS, DOES SOME CORRECTNESS CHECK AND RETURNS THE NUMBER OF COUNTRIES
void print_input_params(int size){
    printf("\nNumber of individuals: %d\n"
           "Number of individuals that are initially infected: %d\n"
           "World Width: %d\n"
           "World Length: %d\n"
           "Countries Width: %d\n"
           "Countries Length: %d\n"
           "Velocity: %lf\n"
           "Maximum Spreading Distance: %d\n"
           "Time Step (seconds): %d\n"
           "Time of Simulation (seconds): %d\n",
           num_individuals,
           inf_individuals,
           world_w,
           world_l,
           country_w,
           country_l,
           velocity,
           spreading_distance,
           t_step,
           t_simulated
    );

    int world_total_area = world_w * world_l;
    int country_total_area = country_w * country_l;
    int num_countries = world_total_area / country_total_area;
    printf("Number of countries: %d\nSize of world: %d\n", num_countries, world_total_area);
    if (num_countries*country_total_area != world_total_area) {
        printf("The number of countries does not match with the size of the world\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
    if (size>num_individuals) {
        printf("You have allocated more processors than individuals, reduce to num_individuals processors at most\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
    if (inf_individuals>num_individuals) {
        printf("There are more infected individuals than there are individuals\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
}

int calc_num_countries(int world_w, int world_l, int country_w, int country_l){
	int world_total_area = world_w * world_l;
    	int country_total_area = country_w * country_l;
    	return (world_total_area / country_total_area);
}

country_type create_country(int id) {
    //Calculating coordinates of the country using the input id
    int world_cols = (world_w / country_w);
    int world_rows = (world_l / country_l);
    //printf("Columns: %d, Rows: %d \n", world_cols, world_rows);
    int nation_col = id % world_cols;
    int nation_row = id / world_cols;
    //printf("Nation id: %d, Col: %d, Row: %d \n", id, nation_col, nation_row);
    country_type country;
    country.country_id = id;
    country.x_start = nation_col * country_w;
    country.x_end = (nation_col + 1) * country_w -1 ;
    country.y_start = nation_row * country_l;
    country.y_end = (nation_row + 1) * country_l -1;
    return country;
}




