//
// Created by Stone on 2023/12/11.
//

#ifndef XTRAGRAPHCOMPUTING_UHC_ENUMERATOR_H
#define XTRAGRAPHCOMPUTING_UHC_ENUMERATOR_H

#include <vector>
#include <unordered_set>
#include "util/sparsepp/spp.h"
#include "util/graph/uncertain_directed_graph.h"
extern bool g_exit;
class UhcEnumerator {
public:
    enum query_method {
        SIMPLE_DFS,
        UHC_ENUM_LEN_PRUNE,
        UHC_ENUM_CSR,
        UHC_ENUM_CSR_OPT,
        UHC_ENUM_CSR_JOIN,
        UHC_ENUM_LEN_PRUNE_JOIN,
        UHC_ENUM_CSR_SMART,
        UHC_ENUM_CSR_PRUNE_OPT,
        UHC_ENUM_CSR_PRUNE,
        UHC_ENUM_CSR_COMPACT,
        UHC_ENUM_CSR_COMPACT_PRUNE,
        UHC_ENUM_CSR_COMPACT_OPT,
        UHC_ENUM_CSR_COMPACT_PRUNE_OPT,
        UHC_ENUM_LEN_PB_PRUNE,

    };

    UncertainDirectedGraph * u_graph_;
    uint32_t num_vertices_;
    uint32_t src_;
    uint32_t dst_;
    uint8_t len_constrain_;
    double gamma_;
    query_method method_type_;
    uint32_t join_count_;

    //basic structure initialized after initializing graph
    bool * sign_; //default value false
    bool * visited_; //default value false
    double * pbs_; //default value 0
    double * pbt_; //default value 0
    double * compacted_pbt_;
    bool * compacted_visited_;

    //basic structure initialized after initializing k and gamma
    std::pair<uint8_t, uint8_t>* distance_;//default value k
    uint32_t * stack_; //dynamic store the path
    uint32_t * actives_offset_; //helper of actives
    uint32_t* bucket_degree_sum_;

    //basic structure dynamic allocate memory
    uint32_t * actives_;
    spp::sparse_hash_map<uint32_t, uint32_t> s_hash_;
    spp::sparse_hash_map<uint32_t, uint32_t> vertex_map_;
    uint32_t * helper_offset_;
    pid *csr_adj_;
    spp::sparse_hash_map<uint32_t, uint32_t> rev_s_hash_;
    uint32_t * rev_helper_offset_;
    pid *rev_csr_adj_;
    //statistic metadata
    uint32_t actives_count_;
    uint64_t result_count_;
    uint64_t accessed_edges_;
    uint64_t concat_path_count_;
    uint64_t index_memory_cost_;
    uint64_t partial_path_memory_cost_;
    uint64_t total_path_memory_cost_;

    //times
    uint64_t query_time_;
    uint64_t preprocess_time_;
    uint64_t bfs_time_;
    uint64_t rev_bfs_time_;
    uint64_t find_actives_time_;
    uint64_t pre_estimate_time_;
    uint64_t build_csr_time_;
    uint64_t eliminate_edges_time_;
    uint64_t find_cutLine_time_;
    uint64_t left_dfs_time_;
    uint64_t right_dfs_time_;
    uint64_t concat_path_time_;
    uint64_t join_time_;

    //statistic counter array
    std::vector<uint64_t> result_count_arr;
    std::vector<uint64_t> actives_count_arr;
    std::vector<uint64_t> accessed_edges_arr;
    std::vector<uint64_t> left_path_count_arr;
    std::vector<uint64_t> right_path_count_arr;
    std::vector<uint64_t> concat_path_count_arr;
    std::vector<uint64_t> index_memory_cost_arr;
    std::vector<uint64_t> partial_path_memory_cost_arr;
    std::vector<uint64_t> total_path_memory_cost_arr;
    std::vector<uint64_t> bfs_time_arr;
    std::vector<uint64_t> rev_bfs_time_arr;
    std::vector<uint64_t> find_actives_time_arr;
    std::vector<uint64_t> pre_estimate_time_arr;
    std::vector<uint64_t> eliminate_edges_time_arr;
    std::vector<uint64_t> build_csr_time_arr;
    std::vector<uint64_t> preprocess_time_arr;
    std::vector<uint64_t> query_time_arr;
    std::vector<uint64_t> find_cutLine_time_arr;
    std::vector<uint64_t> left_dfs_time_arr;
    std::vector<uint64_t> right_dfs_time_arr;
    std::vector<uint64_t> concat_path_time_arr;
    std::vector<uint64_t> join_time_arr;

    //estimated data
    uint8_t cut_position_;
    uint64_t estimated_left_path_count_;
    uint64_t estimated_right_path_count_;
    uint64_t estimated_result_count_;

    //join metadata
    uint32_t *left_relation_;//store left path
    uint64_t left_path_count_; // nums of left path
    uint32_t *left_cursor_; //helper
    uint32_t *left_partial_begin_;
    uint32_t *left_partial_end_;
    uint32_t left_part_length_;
    uint32_t *right_relation_; //store right path
    uint64_t right_path_count_;
    uint32_t *right_cursor_;
    uint32_t right_part_length_;
    uint32_t *right_partial_begin_;
    uint32_t *right_partial_end_;
    std::vector<double> left_pbs; //helper
    std::vector<double> right_pbs;//helper
    spp::sparse_hash_map<uint32_t, std::pair<uint64_t, uint64_t>> index_table_;
    std::unordered_set<uint32_t> connect_vertices;
public:
    UhcEnumerator();
    void init(UncertainDirectedGraph * u_graph);
    void init(query_method method_type, uint8_t len_constrain, double gamma);
    void execute(uint32_t src, uint32_t dst);
    void find_actives();
    void find_actives_opt();
    void find_compacted_actives();
    void build_csr();
    void build_rev_csr();
    void build_compacted_csr();
    void before_compacted_dfs();
    void dfs_by_csr(uint32_t src, uint8_t k, double p);
    void simple_dfs(uint32_t u, uint8_t k, double p);
    void dfs_by_compacted_csr(uint32_t src, uint8_t k, double p);
    void dfs_len_pruning(uint32_t src, uint8_t k, double p);
    void eliminate_edges();
    bool pre_estimator();
    bool estimator();
    void join();
    void len_join();
    void left_dfs(uint32_t u, uint32_t k,double p,std::vector<uint32_t> & left_paths);
    void right_dfs(uint32_t u, uint32_t k,double p,std::vector<uint32_t> & right_paths);
    void len_left_dfs(uint32_t u, uint32_t k,double p,std::vector<uint32_t> & left_paths);
    void len_right_dfs(uint32_t u, uint32_t k,double p,std::vector<uint32_t> & right_paths);
    void single_join();
    void reset_for_next_batch_query();
    void reset_for_next_single_query();
    void clear();
    void update_counter();
    void reset_counter();
    void showPbs();
};
#endif //XTRAGRAPHCOMPUTING_UHC_ENUMERATOR_H
