#include <ydb/meta/meta.h>
#include <ydb/meta/ydb.h>
#include <ydb/meta/log.h>
#include "meta_cache.h"

namespace {

std::mutex SeenLock;
std::unordered_set<TString> SeenIds;

bool HasSeenId(const TString& id) {
    std::lock_guard<std::mutex> lock(SeenLock);
    return SeenIds.count(id) != 0;
}

void MarkIdAsSeen(const TString& id) {
    std::lock_guard<std::mutex> lock(SeenLock);
    SeenIds.insert(id);
}

bool GetCacheOwnership(const TString& id, NMeta::TGetCacheOwnershipCallback cb) {
    if (auto ydbMeta = NMeta::InstanceYdbMeta.lock()) {
        TString endpoint = ydbMeta->ExternalEndpoint;
        TStringBuilder query;
        query << "DECLARE $ID AS Text;\n"
            "DECLARE $FORWARD AS Text;\n";
        if (!HasSeenId(id)) {
            query << "UPSERT INTO `ydb/Forwards.db`(Id) VALUES($ID);\n";
        }
        query << "UPDATE `ydb/Forwards.db` SET Forward=$FORWARD, Deadline=CurrentUtcTimestamp() + Interval('PT60S') WHERE Id=$ID AND (Deadline IS NULL OR (Deadline < CurrentUtcTimestamp()) OR (Forward = $FORWARD AND Deadline < (CurrentUtcTimestamp() + Interval('PT30S'))));\n"
            "SELECT Forward, Deadline FROM `ydb/Forwards.db` WHERE Id=$ID;\n";
        NYdb::TParamsBuilder params;
        params.AddParam("$ID", NYdb::TValueBuilder().Utf8(id).Build());
        params.AddParam("$FORWARD", NYdb::TValueBuilder().Utf8(ydbMeta->ExternalEndpoint).Build());
        ydbMeta->MetaDatabase->ExecuteQuery(query, params.Build()).Subscribe([id, cb = move(cb), endpoint, &as = *ydbMeta->ActorSystem](const NYdb::NQuery::TAsyncExecuteQueryResult& result) mutable {
            NYdb::NQuery::TAsyncExecuteQueryResult resultCopy = result;
            auto res = resultCopy.ExtractValue();
            if (res.IsSuccess()) {
                MarkIdAsSeen(id);
                try {
                    // got result
                    auto resultSet = res.GetResultSet(0);
                    NYdb::TResultSetParser rsParser(resultSet);
                    if (rsParser.TryNextRow()) {
                        TString forward = rsParser.ColumnParser(0).GetOptionalUtf8().value();
                        TInstant deadline = rsParser.ColumnParser(1).GetOptionalTimestamp().value();
                        if (forward == endpoint) {
                            MLOG_D(as, "GetCacheOwnership(" << id << ") - got data (forward to myself until " << deadline << ")");
                            cb({.Deadline = deadline});
                        } else {
                            MLOG_D(as, "GetCacheOwnership(" << id << ") - got data (forward to " << forward << " until " << deadline << ")");
                            cb({.ForwardUrl = forward, .Deadline = deadline});
                        }
                    } else {
                        // no data
                        MLOG_D(as, "GetCacheOwnership(" << id << ") - failed to get data");
                        cb({});
                    }
                } catch (const std::exception& e) {
                    // exception
                    MLOG_D(as, "GetCacheOwnership(" << id << ") - exception: " << e.what());
                    cb({});
                }
            } else {
                // no result
                MLOG_D(as, "GetCacheOwnership(" << id << ") - failed to get result:\n" << (NYdb::TStatus&)res);
                cb({});
            }
        });
    }
    return true;
}

}

namespace NMeta {

NHttp::TCachePolicy GetIncomingMetaCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    if (url.starts_with("/meta/cp_databases")) {
        policy.TimeToExpire = TDuration::Days(3);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    if (url.starts_with("/meta/clusters")) {
        policy.TimeToExpire = TDuration::Days(7);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    // TODO(xenoxeno): temporary disabling cache overrides
    //return NHttp::GetDefaultCachePolicy(request, policy);
    return policy;
}

NHttp::TCachePolicy GetIncomingCachePolicy(const NHttp::THttpRequest* request) {
    return GetIncomingMetaCachePolicy(request); // we only cache what we cache through meta database
}

NHttp::TCachePolicy GetOutgoingCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    if (url == "/viewer/json/tenantinfo") {
        policy.TimeToExpire = TDuration::Minutes(10); // incoming cache will push outgoing cache longer
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
        return policy;
    }
    if (url.EndsWith("/viewer/json/cluster") || url.EndsWith("/viewer/json/sysinfo")) {
        policy.TimeToExpire = TDuration::Minutes(10); // incoming cache will push outgoing cache longer
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
        return policy;
    }
    /* if (url.find("/databases") != TStringBuf::npos) {
        policy.TimeToExpire = TDuration::Hours(24);
        policy.TimeToRefresh = TDuration::Seconds(30);
        policy.KeepOnError = true;
        return policy;
    } */
    return policy;
}

void TYdbMeta::SetupCaches() {
    HttpProxyId = ActorSystem->Register(NHttp::CreateOutgoingHttpCache(HttpProxyId, GetOutgoingCachePolicy));
    HttpIncomingCachedProxyId = ActorSystem->Register(NHttp::CreateIncomingHttpCache(HttpProxyId, GetIncomingCachePolicy));
    if (MetaCache) {
        HttpIncomingCachedProxyId = ActorSystem->Register(NMeta::CreateHttpMetaCache(HttpIncomingCachedProxyId, GetIncomingMetaCachePolicy, GetCacheOwnership));
    }
}

}
