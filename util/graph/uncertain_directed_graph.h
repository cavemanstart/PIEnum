//
// Created by Stone on 2023/12/08.
//

#ifndef UNCERTAIN_DIRECTED_GRAPH_H
#define UNCERTAIN_DIRECTED_GRAPH_H

#include <string>
#include <vector>
#include <random>
typedef std::pair<uint32_t,double> pid;
class UncertainDirectedGraph {
public:
    /**
     * The meta information.
     */
    uint32_t num_vertices_;
    uint32_t num_edges_;
    uint32_t max_num_in_neighbors_;
    uint32_t max_num_out_neighbors_;

    std::vector<uint32_t> out_degree_distribution;
    std::vector<uint32_t> in_degree_distribution;
    /**
     * CSR representation.
     */
    uint32_t* out_offset_;
    pid* out_adj_;
    pid* out_adj_copy_;
    uint32_t* out_degree_;
    uint32_t* pruned_out_degree_;

    uint32_t* in_offset_;
    pid* in_adj_;
    uint32_t* in_degree_;
public:
    explicit UncertainDirectedGraph() : num_vertices_(0), num_edges_(0), max_num_in_neighbors_(0), max_num_out_neighbors_(0),
                               out_offset_(nullptr), out_adj_(nullptr),out_adj_copy_(nullptr), out_degree_(nullptr),pruned_out_degree_(nullptr),
                               in_offset_(nullptr), in_adj_(nullptr), in_degree_(nullptr) {}

    ~UncertainDirectedGraph() {
        free(out_offset_);
        free(in_offset_);
        free(out_adj_);
        free(out_adj_copy_);
        free(in_adj_);
        free(in_degree_);
        free(out_degree_);
        free(pruned_out_degree_);
    }

    inline std::pair<pid *, uint32_t> out_neighbors(uint32_t u) {
        return std::make_pair(out_adj_copy_ + out_offset_[u], out_degree_[u]);
    }
    inline std::pair<pid *, uint32_t> pruned_out_neighbors(uint32_t u) {
        return std::make_pair(out_adj_ + out_offset_[u], pruned_out_degree_[u]);
    }

    inline std::pair<pid*, uint32_t> in_neighbors(uint32_t u) {
        return std::make_pair(in_adj_ + in_offset_[u], in_degree_[u]);
    }

    inline uint32_t num_out_neighbors(uint32_t u) {
        return out_degree_[u];
    }

    inline uint32_t num_in_neighbors(uint32_t u) {
        return in_degree_[u];
    }

    inline uint32_t num_neighbors(uint32_t u) {
        return in_degree_[u] + out_degree_[u];
    }
    inline double generate_random(std::string & distribution){
        std::random_device rd;
        std::mt19937 gen(rd());
        double randomNum;
        double min_val = 0.5;
        double max_val = 1.0;
        if(distribution=="equal"){
            std::uniform_real_distribution<double> dis(0.5, 1.0);
            randomNum = dis(gen);
        }else if(distribution=="normal"){
            std::normal_distribution<double> dis(0.8, 0.1);
            do {
                randomNum = dis(gen);
            } while (randomNum < 0.6 || randomNum > 1.0);
        }else if(distribution=="exp_left"){
            std::uniform_real_distribution<> uniform_dist(0, 1);
            double u = uniform_dist(gen);
            randomNum = min_val + (max_val - min_val) * std::pow(u, 2.5); // 调整指数以集中在左边
        }else if(distribution=="exp_right"){
            std::uniform_real_distribution<> uniform_dist(0, 1);
            double u = uniform_dist(gen);
            randomNum = min_val + (max_val - min_val) * std::pow(u, 0.4); // 调整指数以集中在右边
        }
        // 保留2位小数
        randomNum = std::floor(randomNum * 100 + 0.5) / 100;
        return randomNum;
    }
public:
    /**
     * Graph I/O operations.
     */

    void load_graph(const std::string& graph_dir, std::string & distribution);

    void load_csr(const std::string& graph_dir);

    void store_csr(const std::string& graph_dir);



    void store_edge_list(const std::string& graph_dir);

    void print_metadata();

private:
    /**
     * Graph I/O helper functions.
     */
    void load_csr_degree_file(const std::string &degree_file_path, uint32_t *&degree);
    void load_csr_adj_file(const std::string &adj_file_path, const uint32_t *degree, uint32_t *&offset,
                           pid *&adj);

    void load_edge_list(const std::string &edge_list_file_path,
                        std::vector<std::tuple<uint32_t, uint32_t, double>> &lines,
                        uint32_t &max_vertex_id, std::string & distribution);
    void convert_to_csr(std::vector<std::tuple<uint32_t, uint32_t, double>> &edge_list, uint32_t max_vertex_id,
                        uint32_t *&offset, uint32_t *&degree,  std::pair<uint32_t,double> *&adj, uint32_t key_pos);

    /**
     * Graph manipulation functions.
     */
    void collect_metadata();
};
#endif // DIRECTED_GRAPH_H
