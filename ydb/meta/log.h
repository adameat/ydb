#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>

namespace NMeta {

enum EService : NActors::NLog::EComponent {
    MIN = 257,
    Logger,
    GRPC,
    META,
    MAX
};

inline TString GetLogPrefix() {
    return {};
}

}

using NMeta::GetLogPrefix;

#define BLOG_D(stream) ALOG_DEBUG(EService::META, GetLogPrefix() << stream)
#define BLOG_I(stream) ALOG_INFO(EService::META, GetLogPrefix() << stream)
#define BLOG_W(stream) ALOG_WARN(EService::META, GetLogPrefix() << stream)
#define BLOG_NOTICE(stream) ALOG_NOTICE(EService::META, GetLogPrefix() << stream)
#define BLOG_ERROR(stream) ALOG_ERROR(EService::META, GetLogPrefix() << stream)
#define BLOG_GRPC_D(stream) ALOG_DEBUG(EService::GRPC, GetLogPrefix() << stream)
#define BLOG_GRPC_DC(context, stream) LOG_DEBUG_S(context, EService::GRPC, GetLogPrefix() << stream)
#define MLOG_D(actorsystem, stream) LOG_LOG_S(actorsystem, NActors::NLog::PRI_DEBUG, NMeta::EService::META, GetLogPrefix() << stream)


