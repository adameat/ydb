PROGRAM(ydb-meta)

CFLAGS(
    -DPROFILE_MEMORY_ALLOCATIONS
)

ALLOCATOR(LF_DBG)

SRCS(
    appdata.h
    meta.cpp
    meta.h
    log.h
    signals.h
    ydb.cpp
    ydb.h
    cache/meta_cache.cpp
    cache/meta_cache_database.cpp
    json/filter.cpp
    json/mapper.cpp
    json/merger.cpp
    json/parser.cpp
    json/reducer.cpp
    tokens/meta_tokens.cpp
    handlers/init.cpp
    handlers/common.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/yaml-cpp
    library/cpp/protobuf/json
    library/cpp/getopt
    ydb/library/actors/http
    ydb/library/security
    ydb/core/viewer/yaml
    ydb/meta/protos
    ydb/meta/content
    ydb/public/sdk/cpp/src/client/query
    ydb/public/api/client/nc_private/iam
    ydb/public/api/client/yc_private/iam
)

YQL_LAST_ABI_VERSION()

END()
