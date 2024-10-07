SET(ENUMERATOR_SRC_FILE_LIST main.cpp
        uhc_enumerator.cpp )
ADD_EXECUTABLE(Enumerator.out ${ENUMERATOR_SRC_FILE_LIST})
TARGET_LINK_LIBRARIES(Enumerator.out graph log io)