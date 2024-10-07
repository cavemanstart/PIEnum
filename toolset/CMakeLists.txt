ADD_EXECUTABLE(EdgeList2CSR.out edgelist_to_csr.cpp)
TARGET_LINK_LIBRARIES(EdgeList2CSR.out graph log)

ADD_EXECUTABLE(GenerateVertexPairs.out generate_queries.cpp)
TARGET_LINK_LIBRARIES(GenerateVertexPairs.out graph log)