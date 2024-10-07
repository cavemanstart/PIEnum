//
// Created by Stone on 2023/12/08.
//

#include <cstring>
#include <queue>
#include <algorithm>
#include "graph_operation.h"
#include "util/sparsepp/spp.h"
#include "util/log/log.h"

void
GraphOperation::bfs_distance_from_target(UncertainDirectedGraph *g, uint32_t u, uint8_t * distance,std::vector<uint32_t> & dst,
                                         const uint32_t distance_constraint,const double gamma) {
    std::fill(distance, distance + g->num_vertices_, distance_constraint+1);
    std::vector<double> pbs(g->num_vertices_,0);
    std::queue<uint32_t> q;
    q.push(u);
    distance[u] = 0;
    pbs[u] = 1;
    while (!q.empty()) {
        uint32_t v = q.front();
        q.pop();
        uint8_t dis = distance[v];
        if(dis < distance_constraint){
            auto out_neighbors = g->out_neighbors(v);
            for (uint32_t i = 0; i < out_neighbors.second; ++i) {
                uint32_t vv = out_neighbors.first[i].first;
                if(vv==u)
                    continue;
                double p = out_neighbors.first[i].second;
                double pp = pbs[v] * p;
                if(pp >= gamma){
                    if (distance[vv] == distance_constraint + 1) {
                        distance[vv] = dis + 1;
                        dst.emplace_back(vv);
                        pbs[vv] = pp;
                        q.push(vv);
                    }
                }
            }
        }
    }
}

