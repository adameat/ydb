PROTO_LIBRARY()

SRCS(
    meta.proto
    tokens.proto
)

EXCLUDE_TAGS(GO_PROTO)

PEERDIR(
    ydb/public/api/client/yc_private/ydb/v1
)

END()
