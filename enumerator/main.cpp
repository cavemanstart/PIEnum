//
// Created by Stone on 2023/12/09.
//
#include <chrono>
#include <future>
#include <numeric>
#include <iostream>
#include <iomanip>
#include "algorithm"
#include "util/log/log.h"
#include "util/graph/uncertain_directed_graph.h"
#include "util/io/io.h"
#include "uhc_enumerator.h"
bool execute_within_time_limit(UhcEnumerator* enumerator, uint32_t src, uint32_t dst,
                               uint64_t time_limit) {
    g_exit = false;
    std::future<void> future = std::async(std::launch::async, [enumerator, src, dst](){
        return enumerator->execute(src, dst);
    });

    std::future_status status;
    do {
        status = future.wait_for(std::chrono::seconds(time_limit));
        if (status == std::future_status::deferred) {
            log_error("Deferred.");
            exit(-1);
        } else if (status == std::future_status::timeout) {
            g_exit = true;
        }
    } while (status != std::future_status::ready);
    return g_exit;
}
std::string get_date_time(time_t timestamp = time(NULL))
{
    char buffer[20] = {0};
    struct tm *info = localtime(&timestamp);
    strftime(buffer, sizeof buffer, "%Y/%m/%d %H:%M:%S", info);
    return std::string(buffer);
}
uint64_t accumulate_counter(std::vector<uint64_t> & counter){
    return std::accumulate(counter.begin(),counter.end(), 0ull);
}
double average_time(uint64_t time, uint32_t num_queries){//ms
    return (double) time / num_queries / 1000000.0 ;
}
void excutor(UhcEnumerator &enumerator, std::vector<std::pair<uint32_t, uint32_t>> & queries ,std::string & dir){
    uint32_t terminated_query_count = 0;
    uint32_t num_queries = queries.size();
    uint32_t per_query_time_limit = 1000;
    log_info("num of queries: %u", num_queries);
    for (uint32_t i = 0; i < num_queries; i++) {
        auto query = queries[i];
        if (execute_within_time_limit(&enumerator, query.first, query.second, per_query_time_limit)) {
            terminated_query_count += 1;
        }
        enumerator.update_counter();
//        log_info("src %u, dst %u, num of results: %u",enumerator.src_, enumerator.dst_,enumerator.result_count_);
        enumerator.reset_for_next_single_query();
    }
    std::sort(enumerator.query_time_arr.begin(),enumerator.query_time_arr.end());
    double latency_query_time =(double ) enumerator.query_time_arr[(int)(num_queries*0.99)-1]/ 1000000.0;
    double average_query_time = average_time(accumulate_counter(enumerator.query_time_arr),num_queries) ;
    double average_preprocess_time = average_time(accumulate_counter(enumerator.preprocess_time_arr),num_queries) ;
    double average_enumerate_time = average_query_time - average_preprocess_time;
    double average_find_cutLine_time = average_time(accumulate_counter(enumerator.find_cutLine_time_arr),num_queries);
    double average_join_time = average_time(accumulate_counter(enumerator.join_time_arr),num_queries);
    double average_find_actives_time = average_time( accumulate_counter(enumerator.find_actives_time_arr),num_queries);
    double average_build_csr_time = average_time(accumulate_counter(enumerator.build_csr_time_arr),num_queries);
    double average_bfs_time = average_time( accumulate_counter(enumerator.bfs_time_arr),num_queries);
    double average_rev_bfs_time = average_time( accumulate_counter(enumerator.rev_bfs_time_arr),num_queries);
    double average_left_dfs_time = average_time( accumulate_counter(enumerator.left_dfs_time_arr),num_queries);
    double average_right_dfs_time = average_time( accumulate_counter(enumerator.right_dfs_time_arr),num_queries);
    double average_concat_path_time = average_time( accumulate_counter(enumerator.concat_path_time_arr),num_queries);

    uint64_t average_actives_count = accumulate_counter(enumerator.actives_count_arr)/num_queries;
    uint64_t average_result_count = accumulate_counter(enumerator.result_count_arr)/num_queries;
    uint64_t average_concat_path_count = accumulate_counter(enumerator.concat_path_count_arr)/num_queries;
    uint64_t average_left_path_count = accumulate_counter(enumerator.left_path_count_arr)/num_queries;
    uint64_t average_right_path_count = accumulate_counter(enumerator.right_path_count_arr)/num_queries;
    uint64_t average_accessed_count = accumulate_counter(enumerator.accessed_edges_arr)/num_queries;
    double average_index_memory_cost = (double )accumulate_counter(enumerator.index_memory_cost_arr)/num_queries/1024/1024;
    double average_partial_path_memory_cost =(double ) accumulate_counter(enumerator.partial_path_memory_cost_arr)/num_queries/1024/1024;
    double average_total_path_memory_cost = (double ) accumulate_counter(enumerator.total_path_memory_cost_arr)/num_queries/1024/1024;
    double average_throughput = (double )average_result_count / average_enumerate_time * 1000;
    enumerator.reset_counter();
    enumerator.reset_for_next_batch_query();
    log_info("dump into log file...");
    std::string outfile = dir+"_res_log.txt";
    std::ofstream outs(outfile,std::ios::app);
    outs<<"terminated query ratio: "<<std::fixed<<std::setprecision(2) << (double) terminated_query_count/num_queries <<" ms\n";
    outs<<"average query time: "<<std::fixed<<std::setprecision(2) << average_query_time <<" ms\n";
    outs<<"99% latency query time: "<< std::fixed<<std::setprecision(2) <<latency_query_time<<" ms\n";
    outs<<"average preprocess time: "<< std::fixed<<std::setprecision(2) <<average_preprocess_time <<" ms\n";
    outs<<"average enumerate time: "<< std::fixed<<std::setprecision(2) <<average_enumerate_time <<" ms\n";
    outs<<"average find actives time: "<< std::fixed<<std::setprecision(2) <<average_find_actives_time <<" ms\n";
    outs<<"average build csr time: "<< std::fixed<<std::setprecision(2) <<average_build_csr_time <<" ms\n";
    outs<<"average bfs time: "<< std::fixed<<std::setprecision(2) <<average_bfs_time <<" ms\n";
    outs<<"average reverse bfs time: "<< std::fixed<<std::setprecision(2) <<average_rev_bfs_time <<" ms\n";
    outs<<"average find cut-line time: "<< std::fixed<<std::setprecision(2) << average_find_cutLine_time <<" ms\n";
    outs<<"average join time: "<< std::fixed<<std::setprecision(2) <<average_join_time <<" ms\n";
    outs<<"average left dfs time: "<< std::fixed<<std::setprecision(2) << average_left_dfs_time <<" ms\n";
    outs<<"average right dfs time: "<< std::fixed<<std::setprecision(2) << average_right_dfs_time <<" ms\n";
    outs<<"average concat path time: "<< std::fixed<<std::setprecision(2) << average_concat_path_time <<" ms\n";
    outs<<"average actives count: "<< average_actives_count <<"\n";
    outs<<"average result count: "<< average_result_count <<"\n";
    outs<<"average concat path count: "<< average_concat_path_count <<"\n";
    outs<<"average left path count: "<< average_left_path_count <<"\n";
    outs<<"average right path count: "<< average_right_path_count <<"\n";
    outs<<"average accessed edge count: "<< average_accessed_count <<"\n";
//    outs<<"join method ratio: "<< (double)enumerator.join_count_/num_queries <<"\n";
    outs<<"average index memory cost: "<< std::fixed<<std::setprecision(2)<< average_index_memory_cost <<" mb\n";
    outs<<"average total path memory cost: "<< std::fixed<<std::setprecision(2) << average_total_path_memory_cost <<" mb\n";
    outs<<"average partial path memory cost: "<< std::fixed<<std::setprecision(2) << average_partial_path_memory_cost <<" mb\n";
    outs<<"average throughput: "<< average_throughput <<" per second\n\n";
    outs.close();
}
int main(int argc, char *argv[]) {
    std::string input_graph_folder(argv[1]);
    std::string dir = input_graph_folder.substr(input_graph_folder.size()-2,2);
    std::string input_query_folder(argv[2]);
    std::string query_file = input_query_folder.substr(input_query_folder.size()-2,2);
    std::vector<UhcEnumerator::query_method> methods = {
            UhcEnumerator::query_method::UHC_ENUM_CSR_OPT
            ,UhcEnumerator::query_method::UHC_ENUM_CSR_JOIN
            ,UhcEnumerator::query_method::UHC_ENUM_LEN_PRUNE
            ,UhcEnumerator::query_method::UHC_ENUM_LEN_PRUNE_JOIN
//            ,UhcEnumerator::query_method::UHC_ENUM_CSR
//            UhcEnumerator::query_method::SIMPLE_DFS
//            UhcEnumerator::query_method::UHC_ENUM_CSR_SMART
    };
    std::vector<uint8_t> len_constrains = {8};
    std::vector<double> gammas = {0.7,0.75,0.8,0.85,0.9};
    //init graph
    UncertainDirectedGraph digraph;
    digraph.load_csr(input_graph_folder);
    UhcEnumerator enumerator;
    enumerator.init(&digraph);
    input_query_folder += "/general_pairs.bin";
    std::vector<std::pair<uint32_t, uint32_t>> queries;
    IO::read(input_query_folder, queries);
    double _gamma = 0.8;
    for(auto len_constrain: len_constrains){
        for(auto method : methods){
            std::cout << "method: " << method << '\t';
            std::cout << "length constraint: " << std::to_string(len_constrain) << '\t';
            std::cout << "gamma: " << _gamma << '\n';
            enumerator.init(method,len_constrain,_gamma);
            std::string outfile = dir+"_res_log.txt";
            std::ofstream outs(outfile,std::ios::app);
            outs<<get_date_time()<<"\n";
            outs<<"dataset: "<<dir<<"\t query file: "<<query_file<<"\t method: "<<method
                <<"\t num queries: " << queries.size() <<"\t len constrain: "<<std::to_string(len_constrain) << "\t gamma: "<<std::to_string(_gamma)<<"\n";
            outs.close();
            excutor(enumerator,queries,dir);
        }
    }
//    uint8_t len_constrain = 7;
//    for(auto gamma: gammas){
//        for(auto method : methods){
//            enumerator.init(method,len_constrain,gamma);
//            std::cout << "method: " << method << '\t';
//            std::cout << "length constraint: " <<std::to_string(len_constrain) << '\t';
//            std::cout << "gamma: " << gamma << '\n';
//            std::string outfile = dir + "_res_log.txt";
//            std::ofstream outs(outfile,std::ios::app);
//            outs<<get_date_time()<<"\n";
//            outs<<"dataset: "<<dir<<"\t query file: "<<query_file<<"\t method: "<<method
//                <<"\t num queries: " << queries.size() <<"\t len constrain: "<<std::to_string(len_constrain) << "\t gamma: "<<gamma<<"\n";
//            outs.close();
//            excutor(enumerator,queries,dir);
//        }
//    }
    return 0;
}
