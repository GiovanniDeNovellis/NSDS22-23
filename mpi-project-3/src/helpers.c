#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stddef.h>
#include <math.h>
#define MINUTE 60
#define HOUR (60*MINUTE)
#define DAY (24*HOUR)
#define MONTH (31*DAY)
char directions[] = { 'L', 'R', 'U', 'D'}; //LEFT RIGHT UP DOWN

char status[] = { 'S', 'E', 'I', 'X' }; //SUSCEPTIBLE EXPOSED INFECTED IMMUNE

typedef struct country {
    int country_id;
    int x_start, x_end, y_start, y_end;
} country_type;

typedef struct individual {
    int id;
    int country;
    int x_pos;
    int y_pos;
    int time_status;
    char status;    
    char direction;
} individual_type;

typedef struct summary {
	int country_id;
	int num_susceptibles;
	int num_infected;
	int num_exposed;
	int num_immune;
} country_summary;

individual_type * initialize_individuals(int num_individuals, int num_infected, country_type * countries,int num_countries, int id_adjustment, int process_rank){ 
	individual_type *individuals = malloc(sizeof(individual_type)*num_individuals);
	for(int i=0; i<num_individuals; i++){
		individual_type cur_individual = individuals[i];
		cur_individual.id = process_rank*num_individuals+i+id_adjustment; //The adjustment is used in case the number of individuals is not a multiple of the processor number and therefore
										//process 0 has more individuals than the other processors.
		int country_id= cur_individual.id%num_countries; //EVENLY SPLITTING INDIVIDUALS IN COUNTRIES
		cur_individual.country=country_id;
		int x_start=countries[country_id].x_start;
		int x_end=countries[country_id].x_end;
		int y_start=countries[country_id].y_start;
		int y_end=countries[country_id].y_end;
		//The first num-infected individuals are assigned as infected
		if(i<num_infected)
			cur_individual.status = status[2]; 
		else
			cur_individual.status = status[0];
		cur_individual.direction=directions[(cur_individual.id%4)];
		cur_individual.time_status=0;
		int x = (rand() % (x_end + 1 - x_start)) + x_start;
		int y=  (rand() % (y_end + 1 - y_start)) + y_start;
		cur_individual.x_pos=x;
		cur_individual.y_pos=y;
		individuals[i]=cur_individual;
	}
	return individuals;
}

int distance(individual_type first, individual_type second){
	int x1=first.x_pos;
	int x2=second.x_pos;
	int y1=first.y_pos;
	int y2=second.y_pos;
	return sqrt(pow(x1-x2, 2) + pow(y1-y2, 2));
}

individual_type update_status_individual(individual_type considered_individual, individual_type * individuals, int size, int max_spreading_distance){
	if(considered_individual.status=='S'){ //SUSCEPTIBLE
		int has_been_exposed=0;
		for(int j=0; j<size;j++){ //SEARCHING IF THE INDIVIDUAL HAS BEEN EXPOSED IN THIS TIME STEP
			if(individuals[j].id!=considered_individual.id && individuals[j].status=='I' && distance(considered_individual, individuals[j])<max_spreading_distance){
				has_been_exposed=1;
				break;
			}
		}
		if(has_been_exposed){ //IF EXPOSED, CHANGE STATUS TO EXPOSED AND RESET COUNTER
			considered_individual.status='E';
			considered_individual.time_status=0;
		}
	}
	else if(considered_individual.status=='E'){ //EXPOSED
		int has_been_exposed=0;
		for(int j=0; j<size;j++){//SEARCHING IF THE INDIVIDUAL HAS BEEN EXPOSED IN THIS TIME STEP
			if(individuals[j].id!=considered_individual.id && individuals[j].status=='I' && distance(considered_individual, individuals[j])<max_spreading_distance){
				has_been_exposed=1;
				break;
			}
		}
		if(!has_been_exposed){ //IF NOT EXPOSED, CHANGE STATUS BACK TO SUSCEPTIBLE AND RESET COUNTER
			considered_individual.status='S';
			considered_individual.time_status=0;
		}
		else if(considered_individual.time_status>10*MINUTE){ //IF EXPOSED AND COUNTER REACHED 10 MINUTES, THE INDIVIDUAL BECOMES INFECTED
			considered_individual.status='I';
			considered_individual.time_status=0;
		}
	}
	else if(considered_individual.status=='I'){ //INFECTED
		if(considered_individual.time_status>=10*DAY){ //AN INFECTED INDIVIDUAL BECOMES IMMUNE AFTER 10 DAYS
			considered_individual.status='X';
			considered_individual.time_status=0;
		}
	}
	else{ //IMMUNE
		if(considered_individual.time_status>=3*MONTH){ //AN IMMUNE INDIVIDUAL BECOMES SUSCEPTIBLE AFTER 3 MONTHS
			considered_individual.status='S';
			considered_individual.time_status=0;
		}
	}
	return considered_individual;
}

individual_type update_position(individual_type individual, country_type * countries, int world_w, int world_l,int country_w, double velocity, int time_step){
	country_type country = countries[individual.country];
	if(individual.direction=='R'){
		int new_x = individual.x_pos + velocity*time_step;
		if(new_x>world_w){
			new_x=world_w;
			individual.direction='L'; //BOUNCING POLICY
		}
		else if(new_x>country.x_end){
			individual.country++;
		}
		individual.x_pos=new_x;
	}
	else if(individual.direction=='L'){
		int new_x = individual.x_pos - velocity*time_step;
		if(new_x<0){
			new_x=0;
			individual.direction='R';
		}
		else if(new_x<country.x_start){
			individual.country--;
		}
		individual.x_pos=new_x;
	}
	else if(individual.direction=='U'){
		int new_y = individual.y_pos + velocity*time_step;
		if(new_y>world_l){
			new_y=world_l;
			individual.direction='D';
		}
		else if(new_y>country.y_end){
			individual.country+=world_w/country_w;
		}
		individual.y_pos=new_y;
	}
	else{
		int new_y = individual.y_pos - velocity*time_step;
		if(new_y<0){
			new_y=0;
			individual.direction='U';
		}
		else if(new_y<country.y_start){
			individual.country-=world_w/country_w;
		}
		individual.y_pos=new_y;
	}
	individual.time_status+=time_step;
	return individual;
}

int get_statistic_individuals(individual_type * individuals, int country_id, int num_individual, char status){ 
	int count=0;
	for(int i=0; i<num_individual; i++){
		if(individuals[i].country==country_id && individuals[i].status==status)
			count++;
	}
	return count;
}

int get_statistic_summaries(country_summary * summaries, int country_id, int num_summaries, char status){
	int count=0;
	for(int i=0; i<num_summaries; i++){
		if(summaries[i].country_id==country_id){
			if(status=='S')
				count+=summaries[i].num_susceptibles;
			else if(status=='I')
				count+=summaries[i].num_infected;
			else if(status=='E')
				count+=summaries[i].num_exposed;
			else
				count+=summaries[i].num_immune;
		}
	}
	return count;
}

country_summary * compute_country_summaries(individual_type * individuals, int num_countries, int num_individuals){
	country_summary * summary = malloc(sizeof(country_summary)*num_countries);
	for(int i=0; i<num_countries; i++){
		country_summary s;
		s.country_id=i;
		s.num_susceptibles=get_statistic_individuals(individuals, i, num_individuals,'S'); //SAME CONVENTION AS THE INDIVIDUAL'S STATUS
		s.num_infected=get_statistic_individuals(individuals, i, num_individuals,'I');
		s.num_immune=get_statistic_individuals(individuals, i, num_individuals,'X');
		s.num_exposed=get_statistic_individuals(individuals, i, num_individuals,'E');
		summary[i]=s;
	}
	return summary;
}

void defineStructIndividual(MPI_Datatype *tstype) {
    const int count = 7;
    int blocklens[] = {1,1,1,1,1,1,1};
    MPI_Datatype types[7] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_CHAR, MPI_CHAR};
    MPI_Aint  disps[] = {offsetof(individual_type,id), offsetof(individual_type,country), offsetof(individual_type,x_pos),offsetof(individual_type,y_pos),offsetof(individual_type,time_status),
    	offsetof(individual_type,status),offsetof(individual_type,direction)};

    MPI_Type_create_struct(count, blocklens, disps, types, tstype);
    MPI_Type_commit(tstype);
}

void defineStructSummary(MPI_Datatype *tstype) {
    const int count2 = 5;
    int blocklens[] = {1,1,1,1,1};
    MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Aint  disps[] = {offsetof(country_summary,country_id), offsetof(country_summary,num_susceptibles), offsetof(country_summary,num_infected),
    	offsetof(country_summary,num_exposed),offsetof(country_summary,num_immune)};

    MPI_Type_create_struct(count2, blocklens, disps, types, tstype);
    MPI_Type_commit(tstype);
}




