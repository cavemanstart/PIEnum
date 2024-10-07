//
// Created by Stone on 2023/12/08.
//
#include "util/log/log.h"
#include "util/graph/uncertain_directed_graph.h"

int main(int argc, char *argv[]) {
    std::string input_graph_folder(argv[1]);
    std::string output_graph_folder(argv[2]);
    std::string distribution(argv[3]);
    char skip_character = '#';
    log_info("skip character is %c", skip_character);

    UncertainDirectedGraph digraph;
    digraph.load_graph(input_graph_folder, distribution);
    digraph.print_metadata();
    digraph.store_csr(output_graph_folder);
    log_info("done.");
    return 0;
}
