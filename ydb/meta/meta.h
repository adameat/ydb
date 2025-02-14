#pragma once
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_cache.h>
#include <library/cpp/getopt/last_getopt.h>
//#include <ydb/mvp/core/mvp_log.h>
//#include <ydb/mvp/core/signals.h>
//#include <ydb/mvp/core/appdata.h>
//#include <ydb/mvp/core/mvp_tokens.h>
//#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/rwlock.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>

#include "appdata.h"
#include "signals.h"
#include "ydb.h"
#include "grpc.h"
#include "http.h"
#include <ydb/meta/tokens/meta_tokens.h>

namespace NMeta {

//const TString& GetEServiceName(NActors::NLog::EComponent component);

class TYdbMeta {
protected:
    std::atomic<bool> Quit;
    static void OnTerminate(int);

    NSignals::TSignalHandler<SIGINT, &TYdbMeta::OnTerminate> SignalSIGINT;
    NSignals::TSignalHandler<SIGTERM, &TYdbMeta::OnTerminate> SignalSIGTERM;
    NSignals::TSignalIgnore<SIGPIPE> SignalSIGPIPE;

    int Init(int argc, char** argv);
    int Shutdown();
    void ParseConfig(int argc, char** argv);
    void SetupCaches();
    void SetupMetaHandlers();

public:
    int Run(int argc, char** argv);

    ui16 HttpPort = {};
    ui16 HttpsPort = {};
    bool Http = false;
    bool Https = false;
    bool MLock = false;
    bool StdErr = false;
    void AddSession(const TString& sessionId, const TActorId& actorId);
    void EndSession(const TString& sessionId, const TActorId& actorId);
    TActorId FindSession(const TString& sessionId);
    void UpdateCluster(const TString& name, const NJson::TJsonValue& cluster);
    NJson::TJsonValue GetCluster(const TString& name);

    TString MetaDatabaseEndpoint;
    std::shared_ptr<TYdbLocation> MetaDatabase;
    TString MetaClusterName;
    TGrpc Grpc;
    bool MetaCache = false;
    TString LocalEndpoint; // http(s)://[::1]:8780
    TString ExternalEndpoint; // http(s)://hostname:8780
    TString CaCertificate;
    TString SslCertificate;
    TMetaAppData AppData;
    std::unique_ptr<NActors::TActorSystem> ActorSystem;
    NActors::TActorId HttpProxyId; // raw http proxy
    NActors::TActorId HttpIncomingCachedProxyId; // possible cached http proxy for incoming requests
    TTokensConfig TokensConfig;
    std::unordered_map<TString, TActorId> Sessions;
    std::mutex SessionsMutex;

    struct TClusterInfo {
        NJson::TJsonValue Cluster;
        TInstant UpdatedAt;
    };

    std::unordered_map<TString, TClusterInfo> ClusterInfoCache;
    std::mutex ClusterInfoCacheMutex;
};

extern std::weak_ptr<TYdbMeta> InstanceYdbMeta;

} // namespace NMeta
