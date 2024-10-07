//
// Created by Stone on 2023/12/11.
//

#include <cassert>
#include <chrono>
#include <queue>
#include <iostream>
#include <unordered_set>
#include "uhc_enumerator.h"
#include "util/log/log.h"
#define BUCKET_ID(i, j, l) ((i)*(l) + (j))
bool g_exit = false;
UhcEnumerator::UhcEnumerator() {
    u_graph_ = nullptr;
    sign_ = nullptr;
    visited_ = nullptr;
    pbs_ = nullptr;
    pbt_ = nullptr;
    distance_ = nullptr;
    helper_offset_ = nullptr;
    rev_helper_offset_ = nullptr;
    csr_adj_ = nullptr;
    rev_csr_adj_ = nullptr;
    left_relation_ = nullptr;
    right_relation_ = nullptr;
    actives_ = nullptr;
    actives_offset_ = nullptr;
}
void UhcEnumerator::execute(uint32_t src, uint32_t dst) {
    assert(len_constrain_>0);
    auto start = std::chrono::high_resolution_clock::now();
    src_ = src;
    dst_ = dst;
    if(method_type_ == query_method::SIMPLE_DFS){
        simple_dfs(src_,0,1);
    }
    else if(method_type_ == query_method::UHC_ENUM_LEN_PRUNE){
        auto find_actives_start = std::chrono::high_resolution_clock::now();
        find_actives();
        auto find_actives_end = std::chrono::high_resolution_clock::now();
        find_actives_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(find_actives_end - find_actives_start).count();
        eliminate_edges();
        auto eliminate_edges_end = std::chrono::high_resolution_clock::now();
        eliminate_edges_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(eliminate_edges_end - find_actives_end).count();
        preprocess_time_ = find_actives_time_ + eliminate_edges_time_;
        dfs_len_pruning(src_,0,1);
        memcpy(u_graph_->out_adj_,u_graph_->out_adj_copy_,sizeof (pid)*u_graph_->num_edges_);
    }
    else if (method_type_ == query_method::UHC_ENUM_CSR) {
        auto find_actives_start = std::chrono::high_resolution_clock::now();
        find_actives();
        auto find_actives_end = std::chrono::high_resolution_clock::now();
        find_actives_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(find_actives_end - find_actives_start).count();
        build_csr();
        auto build_csr_end = std::chrono::high_resolution_clock::now();
        build_csr_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(build_csr_end - find_actives_end).count();
        preprocess_time_ = find_actives_time_ + build_csr_time_ ;
        dfs_by_csr(src_, 0, 1);
    } else if (method_type_ == query_method::UHC_ENUM_CSR_OPT) {
        auto find_actives_start = std::chrono::high_resolution_clock::now();
        find_actives_opt();
        auto find_actives_end = std::chrono::high_resolution_clock::now();
        find_actives_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(find_actives_end - find_actives_start).count();
        build_csr();
        auto build_csr_end = std::chrono::high_resolution_clock::now();
        build_csr_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(build_csr_end - find_actives_end).count();
        preprocess_time_ = find_actives_time_ + build_csr_time_ ;
        dfs_by_csr(src_, 0, 1);
    }  else if (method_type_ == query_method::UHC_ENUM_CSR_JOIN) {
        auto find_actives_start = std::chrono::high_resolution_clock::now();
        find_actives_opt();
        auto find_actives_end = std::chrono::high_resolution_clock::now();
        find_actives_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(find_actives_end - find_actives_start).count();
        build_csr();
        build_rev_csr();
        auto build_csr_end = std::chrono::high_resolution_clock::now();
        build_csr_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(build_csr_end - find_actives_end).count();
        preprocess_time_ = find_actives_time_ + build_csr_time_ ;
        estimator();
        auto estimator_end = std::chrono::high_resolution_clock::now();
        find_cutLine_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(estimator_end - build_csr_end).count();
        join();
        auto join_end = std::chrono::high_resolution_clock::now();
        join_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(join_end - estimator_end).count();
    } else if (method_type_ == query_method::UHC_ENUM_LEN_PRUNE_JOIN) {
        auto find_actives_start = std::chrono::high_resolution_clock::now();
        find_actives();
        auto find_actives_end = std::chrono::high_resolution_clock::now();
        find_actives_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(find_actives_end - find_actives_start).count();
        eliminate_edges();
        auto eliminate_edge_end = std::chrono::high_resolution_clock::now();
        eliminate_edges_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(eliminate_edge_end - find_actives_end).count();
        preprocess_time_ = find_actives_time_ + eliminate_edges_time_ ;
        len_join();
        auto join_end = std::chrono::high_resolution_clock::now();
        join_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(join_end - eliminate_edge_end).count();
        memcpy(u_graph_->out_adj_,u_graph_->out_adj_copy_,sizeof (pid)*u_graph_->num_edges_);
    }  else if (method_type_ == query_method::UHC_ENUM_CSR_SMART) {
        auto find_actives_start = std::chrono::high_resolution_clock::now();
        find_actives_opt();
        auto find_actives_end = std::chrono::high_resolution_clock::now();
        find_actives_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(find_actives_end - find_actives_start).count();
        build_csr();
        if(pre_estimator()){
            build_rev_csr();
            auto build_csr_end = std::chrono::high_resolution_clock::now();
            build_csr_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(build_csr_end - find_actives_end).count();
            preprocess_time_ = find_actives_time_ + build_csr_time_;
            if(estimator()){
                join_count_++;
                auto estimator_end = std::chrono::high_resolution_clock::now();
                find_cutLine_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(estimator_end - build_csr_end).count();

                join();
                auto join_end = std::chrono::high_resolution_clock::now();
                join_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(join_end - estimator_end).count();
            }else{
                dfs_by_csr(src,0,1);
            }
        }else{
            auto build_csr_end = std::chrono::high_resolution_clock::now();
            build_csr_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(build_csr_end - find_actives_end).count();
            preprocess_time_ = find_actives_time_ + build_csr_time_;
            dfs_by_csr(src,0,1);
        }
    }
    total_path_memory_cost_ = result_count_ * (len_constrain_ + 1) * sizeof (uint32_t);
    auto end = std::chrono::high_resolution_clock::now();
    query_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}
//initialize enumerator
void UhcEnumerator::init(UncertainDirectedGraph * u_graph){
    u_graph_ = u_graph;
    num_vertices_ = u_graph -> num_vertices_;
    uint64_t size = sizeof(bool) * num_vertices_;
    sign_ = (bool*)malloc(size);
    memset(sign_, false, size);
    visited_ = (bool*)malloc(size);
    memset(visited_, false, size);

    size = sizeof(double) * num_vertices_;
    pbs_ = (double *) malloc(size);
    memset((double *) pbs_,0, size);
    pbt_ = (double *) malloc(size);
    memset((double *) pbt_,0, size);
}
//initialize enumerator for a group query with same len_constrain and gamma
void UhcEnumerator::init(query_method method_type, uint8_t len_constrain, double gamma){
    method_type_ = method_type;
    len_constrain_ = len_constrain;
    gamma_ = gamma;
    join_count_ = 0;
    uint64_t size = sizeof(uint32_t) * (len_constrain_ + 1);
    stack_ = (uint32_t*)malloc(size);
    memset(stack_, 0, size);

    size = sizeof(std::pair<uint8_t, uint8_t>) * num_vertices_;
    distance_ = (std::pair<uint8_t, uint8_t>*) malloc(size);
    memset((uint8_t*)distance_, len_constrain, size);

    actives_offset_ = (uint32_t*)malloc(sizeof(uint32_t) * (len_constrain_ * len_constrain_ + 1));
    memset(actives_offset_, 0, sizeof(uint32_t) * (len_constrain_ * len_constrain_ + 1));

    bucket_degree_sum_ = (uint32_t*)malloc(sizeof(uint32_t) * len_constrain_  * len_constrain_  * len_constrain_);
    memset(bucket_degree_sum_, 0, sizeof(uint32_t) * len_constrain_  * len_constrain_  * len_constrain_);

    result_count_ = 0;
    actives_count_ = 0;
    accessed_edges_ = 0;
    concat_path_count_ = 0;
    left_path_count_ = 0;
    right_path_count_ = 0;
    index_memory_cost_  = 0;
    partial_path_memory_cost_ = 0;
    total_path_memory_cost_ = 0;

    query_time_ = 0;
    preprocess_time_ = 0;
    bfs_time_ = 0;
    rev_bfs_time_ = 0;
    find_actives_time_ = 0;
    pre_estimate_time_ = 0;
    build_csr_time_ = 0;
    eliminate_edges_time_ = 0;
    find_cutLine_time_ = 0;
    left_dfs_time_ = 0;
    right_dfs_time_ = 0;
    concat_path_time_ = 0;
    join_time_ = 0;
}
void UhcEnumerator::reset_for_next_batch_query() {//prepare for next batch query
    free(stack_);
    stack_ = nullptr;
    free(distance_);
    distance_ = nullptr;
    free(actives_offset_);
    actives_offset_ = nullptr;
    free(bucket_degree_sum_);
    bucket_degree_sum_ = nullptr;
}
void UhcEnumerator::reset_for_next_single_query() {//prepare for next query pair

    /*====================fixed size reuse structure=======================*/
    uint64_t size = sizeof(uint32_t) * (len_constrain_ + 1);
    memset(stack_, 0, size);

    size = sizeof(bool) * num_vertices_;
    memset(sign_, false, size);
    memset(visited_, false, size);

    size = sizeof(std::pair<uint8_t, uint8_t>) * num_vertices_;
    memset((uint8_t*) distance_, len_constrain_, size);

    size = sizeof(double) * num_vertices_;
    memset((double *) pbs_, 0, size);
    memset((double *) pbt_, 0, size);

    memset(actives_offset_, 0, sizeof(uint32_t) * (len_constrain_  * len_constrain_ + 1));
    memset(bucket_degree_sum_, 0, sizeof(uint32_t) * len_constrain_  * len_constrain_  * len_constrain_);
    /*====================dynamic size reuse structure=======================*/

    free(helper_offset_);
    helper_offset_ = nullptr;

    free(rev_helper_offset_);
    rev_helper_offset_ = nullptr;

    free(csr_adj_);
    csr_adj_ = nullptr;

    free(rev_csr_adj_);
    rev_csr_adj_ = nullptr;

    s_hash_.clear();
    rev_s_hash_.clear();
    vertex_map_.clear();

    free(actives_);
    actives_ = nullptr;

    free(left_relation_);
    left_relation_ = nullptr;

    free(right_relation_);
    right_relation_ = nullptr;

    left_pbs.clear();
    right_pbs.clear();
    index_table_.clear();
    connect_vertices.clear();

    result_count_ = 0;
    actives_count_ = 0;
    accessed_edges_ = 0;
    concat_path_count_ = 0;
    left_path_count_ = 0;
    right_path_count_ = 0;
    index_memory_cost_  = 0;
    partial_path_memory_cost_ = 0;
    total_path_memory_cost_ = 0;
}
void UhcEnumerator::before_compacted_dfs() {
    uint32_t size = sizeof(bool) * actives_count_;
    compacted_visited_ = (bool*) malloc(size);
    memset(compacted_visited_,false,size);
    actives_count_ --;
}
void UhcEnumerator::update_counter() {
    result_count_arr.emplace_back(result_count_);
    actives_count_arr.emplace_back(actives_count_);
    accessed_edges_arr.emplace_back(accessed_edges_);
    left_path_count_arr.emplace_back(left_path_count_);
    right_path_count_arr.emplace_back(right_path_count_);
    concat_path_count_arr.emplace_back(concat_path_count_);
    index_memory_cost_arr.emplace_back(index_memory_cost_);
    partial_path_memory_cost_arr.emplace_back(partial_path_memory_cost_);
    total_path_memory_cost_arr.emplace_back(total_path_memory_cost_);

    bfs_time_arr.emplace_back(bfs_time_);
    rev_bfs_time_arr.emplace_back(rev_bfs_time_);
    find_actives_time_arr.emplace_back(find_actives_time_);
    pre_estimate_time_arr.emplace_back(pre_estimate_time_);
    find_cutLine_time_arr.emplace_back(find_cutLine_time_);
    eliminate_edges_time_arr.emplace_back(eliminate_edges_time_);
    build_csr_time_arr.emplace_back(build_csr_time_);
    preprocess_time_arr.emplace_back(preprocess_time_);
    query_time_arr.emplace_back(query_time_);
    left_dfs_time_arr.emplace_back(left_dfs_time_);
    right_dfs_time_arr.emplace_back(right_dfs_time_);
    concat_path_time_arr.emplace_back(concat_path_time_);
    join_time_arr.emplace_back(join_time_);
}
void UhcEnumerator::reset_counter() {
    result_count_arr.clear();
    actives_count_arr.clear();
    accessed_edges_arr.clear();
    left_path_count_arr.clear();
    right_path_count_arr.clear();
    concat_path_count_arr.clear();
    index_memory_cost_arr.clear();
    partial_path_memory_cost_arr.clear();
    total_path_memory_cost_arr.clear();

    bfs_time_arr.clear();
    rev_bfs_time_arr.clear();
    find_actives_time_arr.clear();
    pre_estimate_time_arr.clear();
    find_cutLine_time_arr.clear();
    eliminate_edges_time_arr.clear();
    build_csr_time_arr.clear();
    preprocess_time_arr.clear();
    query_time_arr.clear();
    left_dfs_time_arr.clear();
    right_dfs_time_arr.clear();
    concat_path_time_arr.clear();
    join_time_arr.clear();
}
void UhcEnumerator::clear() {
    free(u_graph_);
    u_graph_ = nullptr;
    free(sign_);
    sign_ = nullptr;
    free(visited_);
    visited_ = nullptr;
    free(pbs_);
    pbs_ = nullptr;
    free(pbt_);
    pbt_ = nullptr;
}
void UhcEnumerator::find_actives() {//simple use the prob constraint distance
    auto bfs_start = std::chrono::high_resolution_clock::now();
    uint8_t dis_s1, dis_s2 , dis_t1, dis_t2;
    // bfs from src
    std::queue<uint32_t> q;
    q.push(src_);
    visited_[src_] = true;
    distance_[src_].first = 0;
    pbs_[src_] = 1;
    uint8_t len = 0;
    while (!q.empty()) {
        uint32_t batch_size = q.size();
        while (batch_size--){
            uint32_t v = q.front();
            q.pop();
            visited_[v] = false;
            if(len + 1 < len_constrain_){ //
                auto out_neighbors = u_graph_->out_neighbors(v);
                for(uint32_t i = 0; i < out_neighbors.second; ++i) {
                    uint32_t vv = out_neighbors.first[i].first;
                    if(vv==src_ || vv == dst_)//exclude src and dst
                        continue;
                    double p = out_neighbors.first[i].second;
                    double pp = pbs_[v] * p;
                    if(pp >= gamma_){
                        dis_s2= distance_[vv].first;
                        if (dis_s2 == len_constrain_) {//never accessed
                            distance_[vv].first = len + 1;
                            pbs_[vv] = pp;
                            q.push(vv);
                            visited_[vv] = true;
                        }
                        else {
                            if(pp > pbs_[vv]){ //update pbs[vv]
                                pbs_[vv] = pp;
                                if(!visited_[vv]){
                                    q.push(vv);
                                    visited_[vv] = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        len ++;
    }
    len = 0;
    auto rev_bfs_start = std::chrono::high_resolution_clock::now();
    bfs_time_= std::chrono::duration_cast<std::chrono::nanoseconds>(rev_bfs_start - bfs_start).count();
    // reverse_bfs from dst
    std::vector<std::vector<uint32_t>> temp_buckets(len_constrain_ * len_constrain_);
    q.push(dst_);
    visited_[dst_] = true;
    distance_[dst_].second = 0;
    pbt_[dst_] = 1;
    sign_[dst_] = true;//sign_ include dst_
    while(!q.empty()){
        uint32_t batch_size = q.size();
        while (batch_size--){
            uint32_t v = q.front();
            q.pop();
            visited_[v] = false;
            if(len < len_constrain_ - 1){ //
                auto in_neighbors = u_graph_->in_neighbors(v);
                for(uint32_t i = 0; i < in_neighbors.second; ++i) {
                    uint32_t vv = in_neighbors.first[i].first;
                    if(vv==src_ || vv == dst_)//exclude src and dst
                        continue;
                    double p = in_neighbors.first[i].second;//
                    dis_s2 = distance_[vv].first;
                    if(dis_s2 + len < len_constrain_){
                        double pp = pbt_[v] * p;
                        dis_t2 = distance_[vv].second;
                        if (dis_t2 == len_constrain_) {//never accessed
                            sign_[vv] = true;
                            distance_[vv].second = len + 1;
                            pbt_[vv] = pp;
                            q.push(vv);
                            visited_[vv] = true;
                            actives_count_ ++;
                            uint32_t bucket_id = BUCKET_ID(distance_[vv].first, distance_[vv].second, len_constrain_);
                            temp_buckets[bucket_id].push_back(vv);
                        }
                        else{
                            if(pp > pbt_[vv]){ //update pbs[vv]
                                pbt_[vv] = pp;
                                if(!visited_[vv]){
                                    q.push(vv);
                                    visited_[vv] = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        len ++;
    }
    auto rev_bfs_end = std::chrono::high_resolution_clock::now();
    rev_bfs_time_= std::chrono::duration_cast<std::chrono::nanoseconds>(rev_bfs_end - rev_bfs_start).count();
    actives_count_ ++;
    actives_ = (uint32_t*)malloc(sizeof(uint32_t) * actives_count_);
    actives_[0] = src_;
    uint32_t offset = 1;
    for (uint32_t i = 0; i < len_constrain_; ++i) {
        for (uint32_t j = 0; j < len_constrain_; ++j) {
            uint32_t bucket_id = BUCKET_ID(i, j, len_constrain_);
            actives_offset_[bucket_id] = offset;
            memcpy(actives_ + offset, temp_buckets[bucket_id].data(), sizeof(uint32_t) * temp_buckets[bucket_id].size());
            offset += temp_buckets[bucket_id].size();
            temp_buckets[bucket_id].clear();
        }
    }
    actives_offset_[len_constrain_ * len_constrain_] = offset;
}
void UhcEnumerator::find_actives_opt() {//prob constraint distance + pb constraint
    auto bfs_start = std::chrono::high_resolution_clock::now();
    uint8_t dis_s1, dis_s2 , dis_t1, dis_t2;
    // bfs from src
    std::queue<uint32_t> q;
    q.push(src_);
    visited_[src_] = true;
    distance_[src_].first = 0;
    pbs_[src_] = 1;
    uint8_t len = 0;
    while (!q.empty()) {
        uint32_t batch_size = q.size();
        while (batch_size--){
            uint32_t v = q.front();
            q.pop();
            visited_[v] = false;
            if(len + 1 < len_constrain_){ //
                auto out_neighbors = u_graph_->out_neighbors(v);
                for(uint32_t i = 0; i < out_neighbors.second; ++i) {
                    uint32_t vv = out_neighbors.first[i].first;
                    if(vv==src_ || vv == dst_)//exclude src and dst
                        continue;
                    double p = out_neighbors.first[i].second;
                    double pp = pbs_[v] * p;
                    if(pp >= gamma_){
                        dis_s2= distance_[vv].first;
                        if (dis_s2 == len_constrain_) {//never accessed
                            distance_[vv].first = len + 1;
                            pbs_[vv] = pp;
                            q.push(vv);
                            visited_[vv] = true;
                        }
                        else {
                            if(pp > pbs_[vv]){ //update pbs[vv]
                                pbs_[vv] = pp;
                                if(!visited_[vv]){
                                    q.push(vv);
                                    visited_[vv] = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        len ++;
    }
    len = 0;
    auto rev_bfs_start = std::chrono::high_resolution_clock::now();
    bfs_time_= std::chrono::duration_cast<std::chrono::nanoseconds>(rev_bfs_start - bfs_start).count();
    // reverse_bfs from dst
    std::vector<std::vector<uint32_t>> temp_buckets(len_constrain_ * len_constrain_);
    q.push(dst_);
    visited_[dst_] = true;
    distance_[dst_].second = 0;
    pbt_[dst_] = 1;
    sign_[dst_] = true;//sign_ include dst_
    while(!q.empty()){
        uint32_t batch_size = q.size();
        while (batch_size--){
            uint32_t v = q.front();
            q.pop();
            visited_[v] = false;
            if(len < len_constrain_ - 1){ //
                auto in_neighbors = u_graph_->in_neighbors(v);
                for(uint32_t i = 0; i < in_neighbors.second; ++i) {
                    uint32_t vv = in_neighbors.first[i].first;
                    if(vv==src_ || vv == dst_)//exclude src and dst
                        continue;
                    double p = in_neighbors.first[i].second;//
                    dis_s2 = distance_[vv].first;
                    double pp = pbt_[v] * p;
                    if(dis_s2 + len < len_constrain_ && pp >= gamma_ && pp * pbs_[vv] >=gamma_){
                        dis_t2 = distance_[vv].second;
                        if (dis_t2 == len_constrain_) {//never accessed
                            sign_[vv] = true;
                            distance_[vv].second = len + 1;
                            pbt_[vv] = pp;
                            q.push(vv);
                            visited_[vv] = true;
                            actives_count_ ++;
                            uint32_t bucket_id = BUCKET_ID(distance_[vv].first, distance_[vv].second, len_constrain_);
                            temp_buckets[bucket_id].push_back(vv);
                        }
                        else{
                            if(pp > pbt_[vv]){ //update pbs[vv]
                                pbt_[vv] = pp;
                                if(!visited_[vv]){
                                    q.push(vv);
                                    visited_[vv] = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        len ++;
    }
    auto rev_bfs_end = std::chrono::high_resolution_clock::now();
    rev_bfs_time_= std::chrono::duration_cast<std::chrono::nanoseconds>(rev_bfs_end - rev_bfs_start).count();
    actives_count_ ++;
    actives_ = (uint32_t*)malloc(sizeof(uint32_t) * actives_count_);
    actives_[0] = src_;
    uint32_t offset = 1;
    for (uint32_t i = 0; i < len_constrain_; ++i) {
        for (uint32_t j = 0; j < len_constrain_; ++j) {
            uint32_t bucket_id = BUCKET_ID(i, j, len_constrain_);
            actives_offset_[bucket_id] = offset;
            memcpy(actives_ + offset, temp_buckets[bucket_id].data(), sizeof(uint32_t) * temp_buckets[bucket_id].size());
            offset += temp_buckets[bucket_id].size();
            temp_buckets[bucket_id].clear();
        }
    }
    actives_offset_[len_constrain_ * len_constrain_] = offset;
}
void UhcEnumerator::find_compacted_actives(){
    auto bfs_start = std::chrono::high_resolution_clock::now();
    uint8_t dis_s1, dis_s2 , dis_t1, dis_t2;
    // bfs from src
    std::queue<uint32_t> q;
    q.push(src_);
    visited_[src_] = true;
    distance_[src_].first = 0;
    pbs_[src_] = 1;
    while (!q.empty()) {
        uint32_t v = q.front();
        q.pop();
        visited_[v] = false;
        dis_s1 = distance_[v].first;
        if(dis_s1 < len_constrain_ - 1){ //
            auto out_neighbors = u_graph_->out_neighbors(v);
            for(uint32_t i = 0; i < out_neighbors.second; ++i) {
                uint32_t vv = out_neighbors.first[i].first;
                if(vv==src_ || vv == dst_)//exclude src and dst
                    continue;
                double p = out_neighbors.first[i].second;
                double pp = pbs_[v] * p;
                if(pp >= gamma_){
                    dis_s2 = distance_[vv].first;
                    if (dis_s2 == len_constrain_) {//never accessed
                        distance_[vv].first = dis_s1 + 1;
                        pbs_[vv] = pp;
                        q.push(vv);
                        visited_[vv] = true;
                    }else{
                        if(pp > pbs_[vv]){ //update pbs[vv]
                            pbs_[vv] = pp;
                            if(!visited_[vv]){
                                q.push(vv);
                                visited_[vv]  = true;
                            }
                        }
                    }
                }
            }
        }
    }
    auto rev_bfs_start = std::chrono::high_resolution_clock::now();
    bfs_time_= std::chrono::duration_cast<std::chrono::nanoseconds>(rev_bfs_start - bfs_start).count();
    // reverse_bfs from dst
    q.push(dst_);
    visited_[dst_] = true;
    distance_[dst_].second = 0;
    pbt_[dst_] = 1;
    sign_[dst_] = true;
    vertex_map_[src_] = actives_count_++;
    while(!q.empty()){
        uint32_t v = q.front();
        q.pop();
        visited_[v] = false;
        dis_t1 = distance_[v].second;
        if(dis_t1 < len_constrain_ - 1){ //
            auto in_neighbors = u_graph_->in_neighbors(v);
            for(uint32_t i = 0; i < in_neighbors.second; ++i) {
                uint32_t vv = in_neighbors.first[i].first;
                if(vv==src_ || vv == dst_)//exclude src and dst
                    continue;
                double p = in_neighbors.first[i].second;//
                dis_s2 = distance_[vv].first;
                if(dis_s2 + dis_t1 < len_constrain_){
                    double pp = pbt_[v] * p;
                    dis_t2 = distance_[vv].second;
                    if (dis_t2 == len_constrain_) {//never accessed
                        sign_[vv] = true;
                        vertex_map_[vv] = actives_count_++;
                        distance_[vv].second = dis_t1 + 1;
                        pbt_[vv] = pp;
                        q.push(vv);
                        visited_[vv] = true;
                    }else{
                        if(pp > pbt_[vv]){ //update pbs[vv]
                            pbt_[vv] = pp;
                            if(!visited_[vv]){
                                q.push(vv);
                                visited_[vv]  = true;
                            }
                        }
                    }
                }
            }
        }
    }
    vertex_map_[dst_] = actives_count_++;
    uint64_t size = sizeof(double) * actives_count_;
    compacted_pbt_ = (double *) malloc(size);
    pbt_[src_] = 1;
    for(auto it : vertex_map_){
        compacted_pbt_[it.second] = pbt_[it.first];
    }
    auto rev_bfs_end = std::chrono::high_resolution_clock::now();
    rev_bfs_time_= std::chrono::duration_cast<std::chrono::nanoseconds>(rev_bfs_end - rev_bfs_start).count();
}
void UhcEnumerator::build_csr() {
    std::vector<pid> tmp_flat_adj;
    tmp_flat_adj.reserve(actives_count_ * 10);
    helper_offset_ = (uint32_t*) malloc (sizeof(uint32_t) * len_constrain_ * (actives_count_+1));
    std::vector<std::vector<pid>> temp_adj(len_constrain_);
    uint32_t cur_bucket_id = 0;
    uint32_t cur_bucket_offset = actives_offset_[cur_bucket_id + 1];
    uint32_t cur_bucket_max_degree_offset = cur_bucket_id * len_constrain_;
    uint32_t offset = 0;
    for(uint32_t i=0; i < actives_count_; ++i){
        offset = i*len_constrain_;
        uint32_t v = actives_[i];
        if (i != 0) {
            while (i >= cur_bucket_offset) {
                cur_bucket_id ++;
                cur_bucket_max_degree_offset = cur_bucket_id * len_constrain_;
                cur_bucket_offset = actives_offset_[cur_bucket_id + 1];
            }
        }
        s_hash_[v] = offset;
        auto out_nb = u_graph_->out_neighbors(v);
        for(uint32_t j=0; j<out_nb.second;++j){
            uint32_t vv = out_nb.first[j].first;
            double p = out_nb.first[j].second;
            if(sign_[vv]){//if vv belongs to actives or dst
                temp_adj[distance_[vv].second].emplace_back(vv,p);
            }
        }
        uint32_t local_degree = 0;
        for(uint8_t j=0; j<len_constrain_;++j){
            helper_offset_[offset+j] = tmp_flat_adj.size();
            tmp_flat_adj.insert(tmp_flat_adj.end(),temp_adj[j].begin(),temp_adj[j].end());
            local_degree += temp_adj[j].size();
            temp_adj[j].clear();
            if (i != 0) {
                bucket_degree_sum_[cur_bucket_max_degree_offset + j] += local_degree;
            }
            else {
                bucket_degree_sum_[j] = local_degree;
            }
        }
    }
    offset += len_constrain_;
    helper_offset_[offset] = tmp_flat_adj.size();
    //calculate index memory cost
    //csr
    index_memory_cost_ += tmp_flat_adj.size() * sizeof(pid);
    //help_offset
    index_memory_cost_ += sizeof(uint32_t) * len_constrain_ * (actives_count_+1);
    //hash_set
    index_memory_cost_ += sizeof(uint32_t) * actives_count_ * 2;
    csr_adj_ = (pid*)malloc(sizeof(pid) * tmp_flat_adj.size());
    memcpy(csr_adj_,tmp_flat_adj.data(),sizeof(pid) * tmp_flat_adj.size());

}
void UhcEnumerator::build_rev_csr() {
    actives_[0] = dst_;
    sign_[src_] = true;
    sign_[dst_] = false;
    std::vector<pid> tmp_flat_adj;
    tmp_flat_adj.reserve(actives_count_ * 10);
    rev_helper_offset_ = (uint32_t*)malloc(sizeof(uint32_t) * len_constrain_ * (actives_count_+1));
    std::vector<std::vector<pid>> temp_adj(len_constrain_);
    uint32_t offset = 0;
    for(uint32_t i=0; i<actives_count_;++i){
        offset = i*len_constrain_;
        uint32_t v = actives_[i];
        rev_s_hash_[v] = offset;
        auto in_nb = u_graph_->in_neighbors(v);
        for(uint32_t j=0; j<in_nb.second;++j){
            uint32_t vv = in_nb.first[j].first;
            double p = in_nb.first[j].second;
            if(sign_[vv]){//if vv belongs to actives or src
                temp_adj[distance_[vv].first].emplace_back(vv,p);
            }
        }
        for(uint8_t j=0; j<len_constrain_;++j){
            rev_helper_offset_[offset+j] = tmp_flat_adj.size();
            tmp_flat_adj.insert(tmp_flat_adj.end(),temp_adj[j].begin(),temp_adj[j].end());
            temp_adj[j].clear();
        }
    }
    offset += len_constrain_;
    rev_helper_offset_[offset] = tmp_flat_adj.size();
    rev_csr_adj_ = (pid*)malloc(sizeof(pid) * tmp_flat_adj.size());
    memcpy(rev_csr_adj_,tmp_flat_adj.data(),sizeof(pid) * tmp_flat_adj.size());
}
void UhcEnumerator::build_compacted_csr() {
    std::vector<pid> tmp_flat_adj;
    tmp_flat_adj.reserve(8096);
    helper_offset_ = (uint32_t*)malloc(sizeof(uint32_t) * len_constrain_ * actives_count_);
    std::vector<std::vector<pid>> temp_adj (len_constrain_);
    uint32_t offset = 0;
    for(auto it:vertex_map_){
        if(it.first==dst_) continue;
        uint32_t v = it.first;
        s_hash_[it.second] = offset;
        auto out_nb = u_graph_->out_neighbors(v);
        for(uint32_t j=0; j<out_nb.second;++j){
            uint32_t vv = out_nb.first[j].first;
            double p = out_nb.first[j].second;
            if(sign_[vv]){//if vv belongs to actives or dst
                temp_adj[distance_[vv].second].emplace_back(vertex_map_[vv],p);
            }
        }
        for(uint8_t j=0; j< len_constrain_;++j) {
            helper_offset_[offset+j] = tmp_flat_adj.size();
            tmp_flat_adj.insert(tmp_flat_adj.end(),temp_adj[j].begin(),temp_adj[j].end());
            temp_adj[j].clear();
        }
        offset += len_constrain_;
    }
    helper_offset_[offset] = tmp_flat_adj.size();
    csr_adj_ = (pid*)malloc(sizeof(pid) * tmp_flat_adj.size());
    memcpy(csr_adj_,tmp_flat_adj.data(),sizeof(pid) * tmp_flat_adj.size());
}
void UhcEnumerator::simple_dfs(uint32_t u, uint8_t k, double p){
    stack_[k] = u;
    visited_[u] = true;
    auto out_nb = u_graph_->out_neighbors(u);
    for(uint32_t i = 0; i<out_nb.second; ++i){
        uint32_t v = out_nb.first[i].first;
        double pb = out_nb.first[i].second;
        double p_next = p * pb;
        if(k < len_constrain_ && p_next>= gamma_ && !visited_[v]){
            accessed_edges_ ++;
            if(v==dst_){
                stack_[k+1] = dst_;
                result_count_ ++;
            }else {
                if(k<len_constrain_-1)
                    simple_dfs(v, k + 1, p_next);
            }
        }
    }
    visited_[u] = false;
}
void UhcEnumerator::dfs_by_csr(uint32_t u, uint8_t k, double p) {
    stack_[k] = u;
    visited_[u] = true;
    uint32_t pointer = s_hash_[u];
    uint32_t start  = helper_offset_[pointer];
    uint32_t end = helper_offset_[pointer + len_constrain_ - k];
    for(uint32_t i = start; i<end; ++i) {
        if(g_exit) goto EXIT;
        uint32_t vv = csr_adj_[i].first;
        double pb = csr_adj_[i].second;
        double p_next = p * pb;
        if(p_next >= gamma_ && p_next * pbt_[vv] >= gamma_ && !visited_[vv]) {
            accessed_edges_ ++;
            if(vv == dst_) {
                stack_[k+1] = dst_;
                result_count_++;
            }else {
                if(k + 1 < len_constrain_)
                    dfs_by_csr(vv, k + 1, p_next);
            }
        }
    }
    EXIT:
    visited_[u] = false;
}
void UhcEnumerator::dfs_by_compacted_csr(uint32_t u, uint8_t k, double p) {
    stack_[k] = u;
    compacted_visited_[u] = true;
    uint32_t pointer = s_hash_[u];
    uint32_t start  = helper_offset_[pointer];
    uint32_t end = helper_offset_[pointer + len_constrain_ - k];
    for(uint32_t i = start; i<end; ++i) {
        uint32_t vv = csr_adj_[i].first;
        double pb = csr_adj_[i].second;
        double p_next = p * pb;
        if(p_next >= gamma_ && !compacted_visited_[vv]) {
            accessed_edges_ ++;
            if(vv == actives_count_) {
                stack_[k+1] = actives_count_;
                result_count_++;
            }else {
                dfs_by_compacted_csr(vv, k + 1, p_next);
            }
        }
    }
    compacted_visited_[u] = false;
}
void UhcEnumerator::dfs_len_pruning(uint32_t u, uint8_t k, double p) {
    stack_[k] = u;
    visited_[u] = true;
    auto out_nb = u_graph_->pruned_out_neighbors(u);
    for(uint32_t i = 0; i<out_nb.second; ++i){
        if(g_exit) goto EXIT;
        uint32_t v = out_nb.first[i].first;
        double pb = out_nb.first[i].second;
        double p_next = p * pb;
        if(k + distance_[v].second < len_constrain_ && p_next >= gamma_ && !visited_[v]){
            accessed_edges_ ++;
            if(v==dst_){
                stack_[k+1] = dst_;
                result_count_ ++;
            }else {
                if(k + 1 < len_constrain_)
                    dfs_len_pruning(v, k + 1, p_next);
            }
        }
    }
    EXIT:
    visited_[u] = false;
}
bool UhcEnumerator::pre_estimator() {
    auto estimation_start = std::chrono::high_resolution_clock::now();
    // the first bucket stores src.
    uint32_t preliminary_estimated_result_count_ = bucket_degree_sum_[len_constrain_ - 1];
    for (uint32_t i = 1; i < len_constrain_; ++i) {//可能出现在第i步的点 （j,k）j<=i and k<= len_cons - i
        uint32_t degree_sum = 1;
        uint32_t vertex_sum = 1;
        for (uint32_t j = 1; j <= i; ++j) {
            for (uint32_t k = 1; k <= len_constrain_ - i; ++k) {
                uint32_t budget = len_constrain_ - i - 1;
                uint32_t cur_degree = bucket_degree_sum_[BUCKET_ID(j, k, len_constrain_) * len_constrain_ + budget];
                uint32_t cur_vertex = actives_offset_[BUCKET_ID(j, k, len_constrain_) + 1] - actives_offset_[BUCKET_ID(j, k, len_constrain_)];
                degree_sum += cur_degree;
                vertex_sum += cur_vertex;
            }
        }
        preliminary_estimated_result_count_ *= (degree_sum / vertex_sum) > 1 ? (degree_sum / vertex_sum) : 1;
    }

    auto estimation_end = std::chrono::high_resolution_clock::now();
    pre_estimate_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(estimation_end - estimation_start).count();

    // If the number of estimated results > 100000, then invoke the fine grained optimizer. Our experiment results
    // show that this estimation generally underestimates the number of results.
    return preliminary_estimated_result_count_ > 100000;
}
bool UhcEnumerator::estimator() {
    uint32_t num_vertices = num_vertices_;
    std::vector<uint64_t> current_value(num_vertices, 0);
    std::vector<uint64_t> previous_value(num_vertices, 0);
    // Estimate the number of paths to dst.
    std::vector<uint64_t> num_path_to_dst(len_constrain_ + 1, 0);
    for (uint32_t budget = 1; budget < len_constrain_; ++budget) {
        uint64_t global_sum = 0;
        for (uint32_t i = 1; i <= budget; ++i) {
            for (uint32_t j = 1; j <= len_constrain_ - budget; ++j) {
                uint32_t bucket_id = BUCKET_ID(j, i, len_constrain_);
                for (uint32_t k = actives_offset_[bucket_id]; k < actives_offset_[bucket_id + 1]; ++k) {
                    uint32_t u = actives_[k];
                    // budget is i - 1.
                    uint32_t neighbor_offset = s_hash_[u];
                    uint32_t start = helper_offset_[neighbor_offset];
                    uint32_t end = helper_offset_[neighbor_offset + budget];
                    uint64_t local_sum = 0;
                    for (uint32_t l = start; l < end; ++l){
                        uint32_t v = csr_adj_[l].first;
                        if (v == dst_) {
                            local_sum += 1;
                        } else {
                            local_sum += previous_value[v];
                        }
                    }
                    current_value[u] = local_sum;
                    global_sum += local_sum;
                }
            }
        }
        num_path_to_dst[budget] = global_sum;
        previous_value.swap(current_value);
    }
    {
        estimated_result_count_ = 0;//s->t 最大路径数量
        uint32_t neighbor_offset = s_hash_[src_];
        uint32_t start = helper_offset_[neighbor_offset];
        uint32_t end = helper_offset_[neighbor_offset + len_constrain_];
        for (uint32_t i = start; i < end; ++i) {
            uint32_t u = csr_adj_[i].first;
            if (u == dst_) {
                estimated_result_count_ ++;
            } else {
                estimated_result_count_ += previous_value[u];
            }
        }
    }

    std::fill(previous_value.begin(), previous_value.end(), 0);
    std::fill(current_value.begin(), current_value.end(), 0);

    // Estimate the number of paths
    std::vector<uint64_t> num_path_from_src(len_constrain_ + 1, 0);
    for (uint32_t budget = 1; budget < len_constrain_; ++budget) {
        uint64_t global_sum = 0;
        for (uint32_t i = 1; i <= budget; ++i) {
            for (uint32_t j = 1; j <= len_constrain_ - budget; ++j) {
                uint32_t bucket_id = BUCKET_ID(i, j, len_constrain_);
                for (uint32_t k = actives_offset_[bucket_id]; k < actives_offset_[bucket_id + 1]; ++k) {
                    uint32_t u = actives_[k];
                    // budget is i - 1.
                    uint32_t neighbor_offset = rev_s_hash_[u];
                    uint32_t start = rev_helper_offset_[neighbor_offset];
                    uint32_t end = rev_helper_offset_[neighbor_offset + budget];
                    uint64_t local_sum = 0;
                    for (uint32_t l = start; l < end; ++l) {
                        uint32_t v = rev_csr_adj_[l].first;
                        if (v == src_) {
                            local_sum ++;
                        } else {
                            local_sum += previous_value[v];
                        }
                    }
                    current_value[u] = local_sum;
                    global_sum += local_sum;
                }
            }
        }
        num_path_from_src[budget] = global_sum;
        previous_value.swap(current_value);
    }

    uint64_t min_sum = std::numeric_limits<uint64_t>::max();
    for (uint32_t i = 1; i < len_constrain_; ++i) {
        uint64_t cur_sum = num_path_to_dst[i] + num_path_from_src[len_constrain_ - i];
        if (cur_sum < min_sum) {
            min_sum = cur_sum;
            cut_position_ = len_constrain_ - i;
        }
    }
    estimated_left_path_count_ = num_path_from_src[cut_position_];
    estimated_right_path_count_ = num_path_to_dst[len_constrain_ - cut_position_];

    // Estimated cost of DFS: the total number of intermediate results.
    uint64_t estimated_dfs_cost_ = 0;
    for (uint32_t i = 1; i < len_constrain_; ++i) {
        estimated_dfs_cost_ += num_path_from_src[i];
    }

    // Materialization cost of the partial results + loop over the results. 1.05 is the penalty of checking the duplicate vertices.
    uint64_t estimated_join_cost_ = estimated_left_path_count_ + estimated_right_path_count_ + (uint64_t)(estimated_result_count_ * 1.05);

    return estimated_join_cost_ < estimated_dfs_cost_;
}
void UhcEnumerator::join() {
    // Initialize.
    left_part_length_ = cut_position_ + 1;
    right_part_length_ = len_constrain_ - cut_position_;
    left_path_count_ = 0;//半路径的数量
    right_path_count_ = 0;
    left_partial_begin_ = stack_;
    left_partial_end_ = left_partial_begin_ + left_part_length_;
    right_partial_begin_ = stack_ + left_part_length_;
    right_partial_end_ = right_partial_begin_ + right_part_length_;
    //left_relation 为每条路径分配 left_part_length 的长度，平铺的等长路径
//    left_relation_ = (uint32_t*)malloc(sizeof(uint32_t) * left_part_length_ * estimated_left_path_count_);
    auto left_dfs_start = std::chrono::high_resolution_clock::now();
    // Allocate the memory for the materialization.
    std::vector<uint32_t> left_paths;
    left_paths.reserve(8096);
    std::vector<uint32_t> right_paths;
    right_paths.reserve(8096);
    left_cursor_ = left_relation_;//指向平铺路径的指针
    left_pbs.reserve(estimated_left_path_count_);
//    log_info("estimate left size %u",estimated_left_path_count_);
    right_pbs.reserve(estimated_right_path_count_ + 64);
    left_dfs(src_, 0,1,left_paths);
//    log_info("left size %u",left_path_count_);
    auto left_dfs_end = std::chrono::high_resolution_clock::now();
    left_dfs_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(left_dfs_end - left_dfs_start).count();

    // Allocate the memory for the materialization.
//    right_cursor_ = right_relation_;
    std::vector<uint32_t> right_path;
    right_path.reserve(estimated_right_path_count_ * len_constrain_);
    for(auto & u:connect_vertices){
        uint64_t pre_cnt = right_path_count_;
        right_dfs(u, cut_position_,pbs_[u],right_path);//每一个u -> dfs 搜索
        // temp_count 表示 u->dst的路径数量
        uint64_t temp_count = right_path_count_ - pre_cnt;
        // key = u , value = pair<cursor,temp_count> 其中 cursor 指向u->dst所有路径开始的位置，
        index_table_[u] = std::make_pair(pre_cnt, temp_count);
    }
//    log_info("estimate right size, %u",estimated_right_path_count_);
//    log_info("right size %u",right_path_count_);
//    if(estimated_left_path_count_<left_path_count_) log_error("left false");
//    if(estimated_right_path_count_<right_path_count_) log_error("right false");
    left_relation_ = (uint32_t*)malloc(sizeof(uint32_t) * left_paths.size());
    memcpy(left_relation_,left_paths.data(),sizeof (uint32_t) * left_paths.size());
    right_relation_ = (uint32_t*)malloc(sizeof(uint32_t) * right_path.size());
    memcpy(right_relation_,right_path.data(),sizeof (uint32_t) * right_path.size());
    auto right_dfs_end = std::chrono::high_resolution_clock::now();
    right_dfs_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(right_dfs_end - left_dfs_end).count();
    single_join();
//    estimate_accuracy_ = estimated_result_count_ == 0? 1 : (double)count_ / estimated_result_count_;
    auto join_end = std::chrono::high_resolution_clock::now();
    concat_path_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(join_end - right_dfs_end).count();
    //calculate path memory cost
    partial_path_memory_cost_ = left_path_count_*left_part_length_ + right_path_count_*right_part_length_;
    partial_path_memory_cost_ *= sizeof (uint32_t);
}
void UhcEnumerator::len_join() {
    // Initialize.
    cut_position_ = len_constrain_ / 2;
    left_part_length_ = cut_position_ + 1;
    right_part_length_ = len_constrain_ - cut_position_;
    left_path_count_ = 0;//半路径的数量
    right_path_count_ = 0;
    left_partial_begin_ = stack_;
    left_partial_end_ = left_partial_begin_ + left_part_length_;
    right_partial_begin_ = stack_ + left_part_length_;
    right_partial_end_ = right_partial_begin_ + right_part_length_;
    //left_relation 为每条路径分配 left_part_length 的长度，平铺的等长路径
//    left_relation_ = (uint32_t*)malloc(sizeof(uint32_t) * left_part_length_ * estimated_left_path_count_);
    std::vector<uint32_t> left_paths;
    left_paths.reserve(8096);
    std::vector<uint32_t> right_paths;
    right_paths.reserve(8096);
    auto left_dfs_start = std::chrono::high_resolution_clock::now();
    // Allocate the memory for the materialization.
//    left_cursor_ = left_relation_;//指向平铺路径的指针
    left_pbs.reserve(2048);
//    log_info("estimate left size %u",estimated_left_path_count_);
    right_pbs.reserve( 2048);
    len_left_dfs(src_, 0,1,left_paths);
//    log_info("left size %u",left_path_count_);
    auto left_dfs_end = std::chrono::high_resolution_clock::now();
    left_dfs_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(left_dfs_end - left_dfs_start).count();

    // Allocate the memory for the materialization.
//    right_cursor_ = right_relation_;
    for(auto & u:connect_vertices){
        uint64_t pre_cnt = right_path_count_;
        len_right_dfs(u, cut_position_,pbs_[u],right_paths);//每一个u -> dfs 搜索
        // temp_count 表示 u->dst的路径数量
        uint64_t temp_count = right_path_count_ - pre_cnt;
        // key = u , value = pair<cursor,temp_count> 其中 cursor 指向u->dst所有路径开始的位置，
        index_table_[u] = std::make_pair(pre_cnt, temp_count);
    }
//    log_info("estimate right size, %u",estimated_right_path_count_);
//    log_info("right size %u",right_path_count_);
//    if(estimated_left_path_count_<left_path_count_) log_error("left false");
//    if(estimated_right_path_count_<right_path_count_) log_error("right false");
    left_relation_ = (uint32_t*)malloc(sizeof(uint32_t) * left_paths.size());
    memcpy(left_relation_,left_paths.data(),sizeof (uint32_t) * left_paths.size());
    right_relation_ = (uint32_t*)malloc(sizeof(uint32_t) * right_paths.size());
    memcpy(right_relation_,right_paths.data(),sizeof (uint32_t) * right_paths.size());
    auto right_dfs_end = std::chrono::high_resolution_clock::now();
    right_dfs_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(right_dfs_end - left_dfs_end).count();
    single_join();
//    estimate_accuracy_ = estimated_result_count_ == 0? 1 : (double)count_ / estimated_result_count_;
    auto join_end = std::chrono::high_resolution_clock::now();
    concat_path_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(join_end - right_dfs_end).count();
    partial_path_memory_cost_ = left_path_count_*left_part_length_ + right_path_count_*right_part_length_;
    partial_path_memory_cost_ *= sizeof (uint32_t);
}
void UhcEnumerator::left_dfs(uint32_t u, uint32_t k, double p,std::vector<uint32_t> & left_paths) {
    stack_[k] = u;
    visited_[u] = true;
    uint32_t pointer = s_hash_[u];
    uint32_t start  = helper_offset_[pointer];
    uint32_t end = helper_offset_[pointer + len_constrain_ - k];
    for(uint32_t i = start; i<end; ++i) {
        if(g_exit) goto EXIT;
        uint32_t vv = csr_adj_[i].first;
        double pb = csr_adj_[i].second;
        double p_next = p * pb;
        if(p_next >= gamma_ && p_next * pbt_[vv] >=gamma_ && !visited_[vv]) {
            accessed_edges_ ++;
            if(vv == dst_) {
                stack_[k+1] = dst_;
                result_count_++;
            }else if (k == cut_position_ - 1){
                connect_vertices.insert(vv);
                stack_[cut_position_] = vv;
//                std::copy(left_partial_begin_, left_partial_end_, left_cursor_);
//                left_cursor_ += left_part_length_;
                left_paths.insert(left_paths.end(),left_partial_begin_,left_partial_end_);
                left_pbs.emplace_back(p_next);
                left_path_count_ ++;
            }else{
                left_dfs(vv, k + 1, p_next,left_paths);
            }
        }
    }
    EXIT:
    visited_[u] = false;
}
void UhcEnumerator::len_left_dfs(uint32_t u, uint32_t k, double p,std::vector<uint32_t> & left_paths) {
    stack_[k] = u;
    visited_[u] = true;
    auto out_nb = u_graph_->pruned_out_neighbors(u);
    for(uint32_t i = 0; i<out_nb.second; ++i){
        if(g_exit) goto EXIT;
        uint32_t v = out_nb.first[i].first;
        double pb = out_nb.first[i].second;
        double p_next = p * pb;
        if(k + distance_[v].second < len_constrain_ && p_next >= gamma_ && !visited_[v]){
            accessed_edges_ ++;
            if(v==dst_){
                stack_[k+1] = dst_;
                result_count_ ++;
            }else if(k == cut_position_ - 1) {
                connect_vertices.insert(v);
                stack_[cut_position_] = v;
                left_paths.insert(left_paths.end(),left_partial_begin_,left_partial_end_);
//                left_cursor_ += left_part_length_;
                left_pbs.emplace_back(p_next);
                left_path_count_ ++;
            }else {
                len_left_dfs(v, k + 1, p_next,left_paths);
            }
        }
    }
    EXIT:
    visited_[u] = false;
}
void UhcEnumerator::right_dfs(uint32_t u, uint32_t k, double p, std::vector<uint32_t> & right_paths) {
    stack_[k] = u;
    visited_[u] = true;
    uint32_t pointer = s_hash_[u];
    uint32_t start  = helper_offset_[pointer];
    uint32_t end = helper_offset_[pointer + len_constrain_ - k];
    for(uint32_t i = start; i<end; ++i) {
        if(g_exit) goto EXIT;
        uint32_t vv = csr_adj_[i].first;
        double pb = csr_adj_[i].second;
        double p_next = p * pb;
        if(p_next >= gamma_ && p_next * pbt_[vv] >=gamma_ && !visited_[vv]) {
            accessed_edges_ ++;
            if(vv == dst_) {
                stack_[k+1] = dst_;
                right_paths.insert(right_paths.end(),right_partial_begin_,right_partial_end_);
//                std::copy(right_partial_begin_, right_partial_end_, right_cursor_);
//                right_cursor_ += right_part_length_;
                right_pbs.emplace_back(p_next);
                right_path_count_ ++;
            }else{
                if(k + 1 < len_constrain_)
                    right_dfs(vv, k + 1, p_next,right_paths);
            }
        }
    }
    EXIT:
    visited_[u] = false;
}
void UhcEnumerator::len_right_dfs(uint32_t u, uint32_t k, double p, std::vector<uint32_t> &right_paths) {
    stack_[k] = u;
    visited_[u] = true;
    auto out_nb = u_graph_->pruned_out_neighbors(u);
    for(uint32_t i = 0; i<out_nb.second; ++i){
        if(g_exit) goto EXIT;
        uint32_t v = out_nb.first[i].first;
        double pb = out_nb.first[i].second;
        double p_next = p * pb;
        if(k + distance_[v].second < len_constrain_ && p_next >= gamma_ && !visited_[v]){
            accessed_edges_ ++;
            if(v==dst_){
                stack_[k+1] = dst_;
                right_paths.insert(right_paths.end(),right_partial_begin_,right_partial_end_);
//                std::copy(right_partial_begin_, right_partial_end_, right_cursor_);
//                right_cursor_ += right_part_length_;
                right_pbs.emplace_back(p_next);
                right_path_count_ ++;
            } else {
                if(k + 1 < len_constrain_)
                    len_right_dfs(v, k + 1, p_next,right_paths);
            }
        }
    }
    EXIT:
    visited_[u] = false;
}
void UhcEnumerator::single_join() {
    left_cursor_ = left_relation_;
    for (uint64_t i = 0; i < left_path_count_; ++i) {//遍历每一条左边的半路径
        if(g_exit) break;
        // Initialize visited table.
        for (uint32_t j = 0; j < left_part_length_; ++j) {
            uint32_t u = left_cursor_[j];
            visited_[u] = true;
        }
        double left_pb = left_pbs[i];
        // Join with the partitions.
        uint32_t key = left_cursor_[cut_position_];
        auto partitions = index_table_[key];
        auto offset = partitions.first;
        right_cursor_ = right_relation_ + offset * right_part_length_;
        for (uint64_t j = 0; j < partitions.second; ++j) {//遍历每一条到dst的路径
            double right_pb = right_pbs[offset + j];
            if(left_pb * right_pb/pbs_[key] >= gamma_ ){
                for (uint32_t k = 0; k < right_part_length_; ++k) {
                    uint32_t u = right_cursor_[k];
                    if (u == dst_) {
                        result_count_ ++;
                        concat_path_count_++;
                        break;
                    } else if (visited_[u]) {//判断重复点
                        break;
                    }
                }
            }
            right_cursor_ += right_part_length_;
        }
        // Clear visited table.
        for (uint32_t j = 0; j < left_part_length_; ++j) {
            uint32_t u = left_cursor_[j];
            visited_[u] = false;
        }
        left_cursor_ += left_part_length_;
    }
}
void UhcEnumerator::eliminate_edges() {

    for(uint32_t i = 0; i<actives_count_; ++i){
        uint32_t v  = actives_[i];
        auto out_nb = u_graph_->out_neighbors(v);
        uint32_t idx = 0;
        for(uint32_t j=0 ; j<out_nb.second; ++j){
            uint32_t vv = out_nb.first[j].first;
            double p = out_nb.first[j].second;
            if(sign_[vv]){
                (u_graph_->out_adj_ + u_graph_->out_offset_[v] + idx)->first = vv;
                (u_graph_->out_adj_ + u_graph_->out_offset_[v] + idx)->second = p;
                idx ++;
            }
        }
        u_graph_->pruned_out_degree_[v] = idx;
    }
}

void UhcEnumerator::showPbs() {
    uint32_t idx = 0;
    for(uint32_t i=0 ;i <u_graph_->num_vertices_; ++i){
        if(sign_[i]){
            std::cout<< pbs_ [i]<< " ";
            idx++;
            if(idx%20==0) std::cout<< std::endl;
        }
    }
}