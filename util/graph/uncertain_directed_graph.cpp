//
// Created by Stone on 2023/12/08.
//

#include <fstream>
#include <chrono>
#include <fcntl.h>
#include <vector>
#include <sstream>
#include <cmath>
#include <algorithm>
#include <tuple>
#include <cassert>
#include <iostream>
#include "uncertain_directed_graph.h"
#include "util/log/log.h"

typedef std::pair<uint32_t,double > pid;

void UncertainDirectedGraph::load_graph(const std::string &graph_dir, std::string& distribution) {
    log_info("load graph in edge list from %s", graph_dir.c_str());
    auto start = std::chrono::high_resolution_clock::now();
    std::string edge_list_file_path = graph_dir + std::string("/b_edge_list.bin");
    std::vector<std::tuple<uint32_t,uint32_t,double>> edge_list;
    uint32_t max_vertex_id;
    load_edge_list(edge_list_file_path, edge_list, max_vertex_id, distribution);
    convert_to_csr(edge_list, max_vertex_id, out_offset_, out_degree_, out_adj_, 0);
    convert_to_csr(edge_list, max_vertex_id, in_offset_, in_degree_, in_adj_, 1);
    auto end = std::chrono::high_resolution_clock::now();
    log_info("load graph time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
    collect_metadata();
}

void UncertainDirectedGraph::load_edge_list(const std::string &edge_list_file_path,
                                            std::vector<std::tuple<uint32_t, uint32_t, double>> &lines,
                                            uint32_t &max_vertex_id, std::string & distribution) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream ifs(edge_list_file_path);
    max_vertex_id = 0;
    std::string tmp_str;
    uint32_t src, dst;
    uint32_t line_count = 0;
    while (std::getline(ifs, tmp_str)) {
        line_count += 1;
        if (tmp_str[0] != '#') {
            std::stringstream ss(tmp_str);
//            if (!(ss >> src >> dst)) {
//                log_error("Cannot convert line %u to edge.", line_count);
//                exit(-1);
//            }
            ss>>src>>dst;
//            std::string tmp;
//            std::getline(ss,tmp,',');
//            src  = stoul(tmp);
//            std::getline(ss,tmp,',');
//            dst = stoul(tmp);
            lines.emplace_back(src, dst, generate_random(distribution));
            if (src > max_vertex_id) {
                max_vertex_id = src;
            }
            if (dst > max_vertex_id) {
                max_vertex_id = dst;
            }
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    log_info("read edge list file time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}

void UncertainDirectedGraph::convert_to_csr(std::vector<std::tuple<uint32_t, uint32_t, double>> &edge_list, uint32_t max_vertex_id,
                                            uint32_t *&offset, uint32_t *&degree, pid *&adj, uint32_t key_pos) {
    auto start = std::chrono::high_resolution_clock::now();
    if (key_pos == 0) {
        std::sort(edge_list.begin(), edge_list.end(),
                  [](const std::tuple<uint32_t, uint32_t, double> &left, const std::tuple<uint32_t, uint32_t, double> &right) {
                      if (std::get<0>(left) == std::get<0>(right)) {
                          return std::get<1>(left) < std::get<1>(right);
                      }
                      return std::get<0>(left) < std::get<0>(right);
                  });
    }
    else {
        std::sort(edge_list.begin(), edge_list.end(),
        [](const std::tuple<uint32_t, uint32_t, double> &left, const std::tuple<uint32_t, uint32_t, double> &right) {
            if (std::get<1>(left) == std::get<1>(right)) {
                return std::get<0>(left) < std::get<0>(right);
            }
            return std::get<1>(left) < std::get<1>(right);
        });
    }

    num_vertices_ = max_vertex_id + 1;
    num_edges_ = 0;
    uint64_t size = sizeof(uint32_t) * num_vertices_;
    degree = (uint32_t*) malloc(size);
    memset(degree, 0, size);
    std::vector<std::vector< std::pair<uint32_t, double>>> adj_arr(num_vertices_);

    std::pair<uint32_t, uint32_t> prev_edge = std::make_pair(num_vertices_, num_vertices_);
    for (const auto &edge : edge_list) {
        // Remove parallel edges.
        if (!(prev_edge.first == std::get<0>(edge) && prev_edge.second == std::get<1>(edge))) {
            prev_edge = std::make_pair(std::get<0>(edge),std::get<1>(edge));
            uint32_t src, dst;
            src = std::get<0>(edge);
            dst = std::get<1>(edge);
            // Remove self loops.
            if (src != dst) {
                if (key_pos == 0) {
                    degree[src] ++;
                    adj_arr[src].emplace_back(dst,std::get<2>(edge));
                }
                else {
                    degree[dst] ++;
                    adj_arr[dst].emplace_back(src,std::get<2>(edge));
                }
                num_edges_ ++;
            }
        }
    }

    size = sizeof(uint32_t) * (num_vertices_ + 1);
    offset = (uint32_t*) malloc(size);
    offset[0] = 0;
    for (uint32_t i = 0; i < num_vertices_; ++i) {
        offset[i + 1] = offset[i] + degree[i];
    }

    std::vector<pid> tmp_flat_adj;
    tmp_flat_adj.reserve(num_edges_);
    for (uint32_t i = 0; i < num_vertices_; ++i) {
        tmp_flat_adj.insert(tmp_flat_adj.end(),adj_arr[i].begin(),adj_arr[i].end());
    }
    adj = (pid*) malloc(sizeof(pid) * tmp_flat_adj.size());
    memcpy(adj,tmp_flat_adj.data(),sizeof (pid)*tmp_flat_adj.size());
    auto end = std::chrono::high_resolution_clock::now();
    log_info("convert edge list to CSR time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}

void UncertainDirectedGraph::store_csr(const std::string &graph_dir) {
    log_info("store graph as CSR into %s", graph_dir.c_str());
    auto start = std::chrono::high_resolution_clock::now();

    std::string out_degree_file_path = graph_dir + std::string("/b_out_degree.bin");
    std::string out_adj_file_path = graph_dir + std::string("/b_out_adj.bin");
    std::string in_degree_file_path = graph_dir + std::string("/b_in_degree.bin");
    std::string in_adj_file_path = graph_dir + std::string("/b_in_adj.bin");

    {
        std::ofstream degree_file(out_degree_file_path, std::ios::binary);
        uint32_t int_size = sizeof(uint32_t);
        uint64_t size = (uint64_t) int_size * num_vertices_;
        degree_file.write(reinterpret_cast<const char *>(&int_size), 4);
        degree_file.write(reinterpret_cast<const char *>(&num_vertices_), 4);
        degree_file.write(reinterpret_cast<const char *>(&num_edges_), 4);
        degree_file.write(reinterpret_cast<const char *>(out_degree_), size);
    }
    {
        std::ofstream degree_file(in_degree_file_path, std::ios::binary);
        uint32_t int_size = sizeof(uint32_t);
        uint64_t size = (uint64_t) int_size * num_vertices_;
        degree_file.write(reinterpret_cast<const char *>(&int_size), 4);
        degree_file.write(reinterpret_cast<const char *>(&num_vertices_), 4);
        degree_file.write(reinterpret_cast<const char *>(&num_edges_), 4);
        degree_file.write(reinterpret_cast<const char *>(in_degree_), size);
    }
    {
        // Output out_neighbors.
        std::ofstream adj_file(out_adj_file_path, std::ios::binary);
        uint64_t size = sizeof(pid) * num_edges_;
        adj_file.write(reinterpret_cast<const char *>(out_adj_), size);
    }
    {
        // Output in_neighbors.
        std::ofstream adj_file(in_adj_file_path, std::ios::binary);
        uint64_t size = sizeof(pid) * num_edges_;
        adj_file.write(reinterpret_cast<const char *>(in_adj_), size);
    }

    auto end = std::chrono::high_resolution_clock::now();
    log_info("store graph time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}

void UncertainDirectedGraph::load_csr(const std::string &graph_dir) {
    printf("load graph in CSR from %s", graph_dir.c_str());
    auto start = std::chrono::high_resolution_clock::now();

    std::string out_degree_file_path = graph_dir + std::string("/b_out_degree.bin");
    std::string out_adj_file_path = graph_dir + std::string("/b_out_adj.bin");
    std::string in_degree_file_path = graph_dir + std::string("/b_in_degree.bin");
    std::string in_adj_file_path = graph_dir + std::string("/b_in_adj.bin");

    load_csr_degree_file(out_degree_file_path, out_degree_);
    uint64_t size = sizeof(uint32_t) * num_vertices_;
    pruned_out_degree_ = (uint32_t*) malloc(size);
    memset(pruned_out_degree_, 0, size);
    size = sizeof(pid) * (uint64_t) (num_edges_);
    out_adj_copy_ =(pid *) malloc(size);
    load_csr_adj_file(out_adj_file_path, out_degree_, out_offset_, out_adj_);
    memcpy(out_adj_copy_,out_adj_,size);
    load_csr_degree_file(in_degree_file_path, in_degree_);
    load_csr_adj_file(in_adj_file_path, in_degree_, in_offset_, in_adj_);

    auto end = std::chrono::high_resolution_clock::now();
//    log_info("load graph time: %.3lf seconds",
//             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
    collect_metadata();
}

void UncertainDirectedGraph::load_csr_degree_file(const std::string &degree_file_path, uint32_t *&degree) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream deg_file(degree_file_path, std::ios::binary);
    int int_size;
    deg_file.read(reinterpret_cast<char *>(&int_size), 4);
    deg_file.read(reinterpret_cast<char *>(&num_vertices_), 4);
    deg_file.read(reinterpret_cast<char *>(&num_edges_), 4);

    uint64_t size = ((uint64_t) num_vertices_) * int_size;
    degree = (uint32_t*) malloc(size);
    deg_file.read(reinterpret_cast<char *>(degree), size);

    auto end = std::chrono::high_resolution_clock::now();
//    log_info("read degree file time: %.3lf seconds",
//             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
    std::cout<<std::endl;
}

void UncertainDirectedGraph::load_csr_adj_file(const std::string &adj_file_path, const uint32_t *degree, uint32_t *&offset,
                                      pid *&adj) {
    auto start = std::chrono::high_resolution_clock::now();

    // CSR representation
    uint64_t size = sizeof(uint32_t) * (uint64_t) (num_vertices_ + 1);
    offset = (uint32_t *) malloc(size);
    offset[0] = 0;
    for (auto i = 0u; i < num_vertices_; ++i) {
        offset[i + 1] = offset[i] + degree[i];
    }
    std::ifstream adj_file(adj_file_path, std::ios::binary);

    size = sizeof(pid) * (uint64_t) (num_edges_);
    adj = (pid *) malloc(size);
    adj_file.read(reinterpret_cast<char *>(adj),size);
    auto end2 = std::chrono::high_resolution_clock::now();
//    log_info("read adj file time: %.3lf seconds",
//            std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start).count() / 1000.0);
}


void UncertainDirectedGraph::store_edge_list(const std::string &graph_dir) {
    log_info("store graph as edge list into %s", graph_dir.c_str());
    auto start = std::chrono::high_resolution_clock::now();
    std::string edge_list_file_path = graph_dir + std::string("/b_edge_list.bin");
    std::ofstream edge_list_file(edge_list_file_path);
    edge_list_file << '#' << num_vertices_ << ' ' << num_edges_ << '\n';

    for (uint32_t src = 0; src < num_vertices_; ++src) {
        for (uint32_t i = out_offset_[src]; i < out_offset_[src + 1]; ++i) {
            auto dst = out_adj_[i];
            edge_list_file << src << ' ' << dst.first << '\n';
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    log_info("store graph time: %.3lf seconds",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0);
}



void UncertainDirectedGraph::print_metadata() {
    log_info("graph metadata: num of vertices = %u, num of edges = %u, average degree = %.2lf, max out degree = %u, max in degree = %u",
            num_vertices_, num_edges_, (double) 2 * num_edges_ / num_vertices_, max_num_out_neighbors_, max_num_in_neighbors_);

    std::stringstream ss;
    for (uint32_t i = 0; i < out_degree_distribution.size(); ++i) {
        if (out_degree_distribution[i] == 0)
            continue;

        if (i == 0) {
            ss << "{[0,2), num = " << out_degree_distribution[i] << "}, ";
        }
        else if (i == out_degree_distribution.size() - 1) {
            ss << "{[" << (uint32_t)exp2(i) << ",inf), num = " << out_degree_distribution[i] << "}";
        }
        else {
            ss << "{[" << (uint32_t)exp2(i) << "," << (uint32_t)exp2(i + 1) << "), num = " << out_degree_distribution[i] << "}, ";
        }
    }
    log_info("out degree distribution: %s", ss.str().c_str());
    ss.clear();

    for (uint32_t i = 0; i < in_degree_distribution.size(); ++i) {
        if (in_degree_distribution[i] == 0)
            continue;

        if (i == 0) {
            ss << "{[0,2), num = " << in_degree_distribution[i] << "}, ";
        }
        else if (i == in_degree_distribution.size() - 1) {
            ss << "{[" << (uint32_t)exp2(i) << ",inf), num = " << in_degree_distribution[i] << "}";
        }
        else {
            ss << "{[" << (uint32_t)exp2(i) << "," << (uint32_t)exp2(i + 1) << "), num = " << out_degree_distribution[i] << "}, ";
        }
    }
    log_info("in degree distribution: %s", ss.str().c_str());
}

void UncertainDirectedGraph::collect_metadata() {
    uint32_t bucket_num = 21;
    out_degree_distribution.resize(bucket_num, 0);
    in_degree_distribution.resize(bucket_num, 0);

    for (uint32_t u = 0; u < num_vertices_; ++u) {
        uint32_t out_degree = num_out_neighbors(u);
        uint32_t in_degree = num_in_neighbors(u);

        if (out_degree < 2) {
            out_degree_distribution[0] += 1;
        }
        else {
            uint32_t bucket_id = (uint32_t)log2(out_degree);
            if (bucket_id >= bucket_num - 1) {
                out_degree_distribution[bucket_num - 1] += 1;
            }
            else {
                out_degree_distribution[bucket_id] += 1;
            }
        }

        if (out_degree > max_num_out_neighbors_) {
            max_num_out_neighbors_ = out_degree;
        }

        if (in_degree < 2) {
            in_degree_distribution[0] += 1;
        }
        else {
            uint32_t bucket_id = (uint32_t)log2(in_degree);
            if (bucket_id >= bucket_num - 1) {
                in_degree_distribution[bucket_num - 1] += 1;
            }
            else {
                in_degree_distribution[bucket_id] += 1;
            }
        }

        if (in_degree > max_num_in_neighbors_) {
            max_num_in_neighbors_ = in_degree;
        }
    }
}


