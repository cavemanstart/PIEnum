//
// Created by Stone on 2023/12/08.
//

#include <queue>
#include <unordered_set>
#include <random>
#include <iterator>
#include <fstream>
#include <chrono>
#include <iostream>

#include "util/log/log.h"
#include "util/graph/uncertain_directed_graph.h"
#include "util/graph/graph_operation.h"

typedef std::pair<uint32_t, uint32_t> pi;

template<typename Iter, typename RandomGenerator>
Iter select_randomly(Iter start, Iter end, RandomGenerator& g) {
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(g));
    return start;
}

template<typename Iter>
Iter select_randomly(Iter start, Iter end) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    return select_randomly(start, end, gen);
}

void generate_pairs(uint32_t num_pairs, std::vector<uint32_t>& src_array, std::vector<uint32_t>& dst_array,
        std::vector<pi>& pair_array) {
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t step_length = num_pairs * 0.2;

    for (uint32_t i = 0; i < num_pairs; ++i) {
        uint32_t src, dst;
        do {
            src = *select_randomly(src_array.begin(), src_array.end());
            dst = *select_randomly(dst_array.begin(), dst_array.end());
        } while (src == dst);

        pair_array.emplace_back(std::make_pair(src, dst));

        if ((i + 1) % step_length == 0) {
            log_info("%u pairs have been generated", i + 1);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    log_info("generate pairs time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}

void generate_pairs_within_constrain(UncertainDirectedGraph * digraph, uint32_t num_pairs, std::vector<uint32_t>& src_array,
        std::vector<pi>& pair_array, uint32_t length_constrain , double gamma) {
    auto start = std::chrono::high_resolution_clock::now();
    uint32_t step_length = num_pairs * 0.2;
    uint8_t * distance;
    distance = (uint8_t*)malloc(sizeof(uint8_t)*(digraph->num_vertices_));
    log_info("Generator begin\n");
    for (uint32_t i = 0; i < num_pairs; ) {
        uint32_t src, dst;
        src = *select_randomly(src_array.begin(), src_array.end());
        std::vector<uint32_t> vec_dst;
        vec_dst.reserve(8096);
        GraphOperation::bfs_distance_from_target(digraph, src, distance, vec_dst,
                                                length_constrain, gamma);
        if(vec_dst.empty())
            continue;
        i++;
        dst = * select_randomly(vec_dst.begin(), vec_dst.end());
        pair_array.emplace_back(std::make_pair(src, dst));
        vec_dst.clear();
        if ((i + 1) % step_length == 0) {
            log_info("%u pairs have been generated", i + 1);
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    log_info("generate pairs time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}

void output_to_file(const std::string& file_path, std::vector<pi>& pair_array) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ofstream ofs(file_path, std::ios::binary);
    uint32_t pair_size = sizeof(pi);
    uint32_t length = pair_array.size();
    uint64_t size = (uint64_t)length * pair_size;

    ofs.write(reinterpret_cast<const char *>(&pair_size), 4);
    ofs.write(reinterpret_cast<const char *>(&length), 4);
    ofs.write(reinterpret_cast<const char *>(&pair_array.front()), size);

    auto end = std::chrono::high_resolution_clock::now();
    log_info("store pairs file time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}

int main(int argc, char *argv[]) {
    std::string input_graph_folder(argv[1]);
    std::string output_folder(argv[2]);
    uint32_t num_pairs = 1000;
    double hot_vertex_percentage = 0.1;
    uint32_t length_constrain = 4;
    double gamma = 0.5;
    if (argc > 3) {
        num_pairs = atoi(argv[3]);
    }
    if (argc > 4) {
        length_constrain = atoi(argv[4]);
    }
    if (argc > 5) {
        gamma = atof(argv[5]);
    }
    UncertainDirectedGraph digraph;
    digraph.load_csr(input_graph_folder);
//    digraph.print_metadata();
    std::vector<uint32_t> vertices (digraph.num_vertices_);
    for(uint32_t i=0; i< vertices.size(); ++i){
        vertices[i] = i;
    }
    // Separate hot vertices and unhot vertices.
//    log_info("num of pairs = %u, percentage of hot vertices = %.2lf", num_pairs, hot_vertex_percentage);
//    log_info("separate hot vertices with unhot vertices...");
    auto start = std::chrono::high_resolution_clock::now();

//    uint32_t num_vertices = digraph.num_vertices_;
//    uint32_t num_hot_vertices = (uint32_t)(num_vertices * hot_vertex_percentage);
//
//    std::priority_queue<pi, std::vector<pi>, std::greater<pi>> pq;
//    for (uint32_t u = 0; u < num_hot_vertices; ++u) {
//        pq.emplace(std::make_pair(digraph.num_neighbors(u), u));
//    }
//
//    for (uint32_t u = num_hot_vertices; u < num_vertices; ++u) {
//        auto top = pq.top();
//        uint32_t cur_degree = digraph.num_neighbors(u);
//
//        if (cur_degree > top.first) {
//            pq.pop();
//            pq.emplace(std::make_pair(cur_degree, u));
//        }
//    }
//
//    std::vector<uint32_t> hot_vertices;
//    std::unordered_set<uint32_t> hot_vertices_flag;
//    while (!pq.empty()) {
//        auto top = pq.top();
//        hot_vertices.emplace_back(top.second);
//        hot_vertices_flag.emplace(top.second);
//        pq.pop();
//    }
//
//    auto end1 = std::chrono::high_resolution_clock::now();
//    log_info("obtain hot vertices time: %.3lf seconds",
//             std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start).count() / 1000.0);
//
//    std::vector<uint32_t> unhot_vertices;
//    for (uint32_t u = 0; u < num_vertices; ++u) {
//        if (hot_vertices_flag.find(u) == hot_vertices_flag.end()) {
//            unhot_vertices.emplace_back(u);
//        }
//    }
//
//    auto end2 = std::chrono::high_resolution_clock::now();
//    log_info("obtain unhot vertices time: %.3lf seconds",
//             std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start).count() / 1000.0);

//    // Generate pairs.
    std::vector<pi> pairs;
//
//    // 1. hot to hot
//    log_info("generate hot to hot pairs...");
//    std::string output_file_path = output_folder + std::string("/hot2hot_pairs.bin");
//    //generate_pairs(num_pairs, hot_vertices, hot_vertices, pairs);
//
//    //use bfs to generate vertex pairs whose distance is within the length constrain
//    generate_pairs_within_constrain(&digraph, num_pairs, hot_vertices, hot_vertices, pairs, length_constrain,gamma);
//    output_to_file(output_file_path, pairs);
//    pairs.clear();
//
//    // 2. hot to unhot
//    log_info("generate hot to unhot pairs...");
//    output_file_path = output_folder + std::string("/hot2unhot_pairs.bin");
//    //generate_pairs(num_pairs, hot_vertices, unhot_vertices, pairs);
//    generate_pairs_within_constrain(&digraph, num_pairs, hot_vertices, unhot_vertices, pairs, length_constrain,gamma);
//    output_to_file(output_file_path, pairs);
//    pairs.clear();
//
//    // 3. unhot to hot
//    log_info("generate unhot to hot pairs...");
//    output_file_path = output_folder + std::string("/unhot2hot_pairs.bin");
//    //generate_pairs(num_pairs, unhot_vertices, hot_vertices, pairs);
//    generate_pairs_within_constrain(&digraph, num_pairs, unhot_vertices, hot_vertices, pairs, length_constrain,gamma);
//    output_to_file(output_file_path, pairs);
//    pairs.clear();
//
    // 4. unhot to unhot
//    log_info("generate unhot to unhot pairs...");
//    std::string output_file_path = output_folder + std::string("/unhot2unhot_pairs.bin");
//    //generate_pairs(num_pairs, unhot_vertices, unhot_vertices, pairs);
//    generate_pairs_within_constrain(&digraph, num_pairs, unhot_vertices, pairs, length_constrain,gamma);
//    output_to_file(output_file_path, pairs);
//    pairs.clear();

    log_info("generate general pairs...");
    std::string output_file_path = output_folder + std::string("/general_pairs.bin");
    //generate_pairs(num_pairs, unhot_vertices, unhot_vertices, pairs);
    generate_pairs_within_constrain(&digraph, num_pairs, vertices, pairs, length_constrain,gamma);
    output_to_file(output_file_path, pairs);
    pairs.clear();
    log_info("done.");
    return 0;
}
