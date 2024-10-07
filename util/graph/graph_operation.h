//
// Created by Stone on 2023/12/08.
//

#ifndef XTRAGRAPHCOMPUTING_GRAPH_OPERATION_H
#define XTRAGRAPHCOMPUTING_GRAPH_OPERATION_H
#include "util/sparsepp/spp.h"
#include "uncertain_directed_graph.h"
class GraphOperation {
public:
    static void bfs_distance_from_target(UncertainDirectedGraph *g, uint32_t u, uint8_t *distance, std::vector<uint32_t> & dst,
                                         uint32_t distance_constraint, double gamma);

};


#endif //XTRAGRAPHCOMPUTING_GRAPH_OPERATION_H
