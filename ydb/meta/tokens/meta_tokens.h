#pragma once
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/spinlock.h>
#include <util/generic/queue.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/meta/protos/tokens.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/nc_private/iam/token_service.grpc.pb.h>
#include <ydb/public/api/client/nc_private/iam/token_exchange_service.grpc.pb.h>
#include <ydb/public/api/protos/ydb_auth.pb.h>
#include <ydb/meta/grpc.h>

namespace NMeta {

class TMetaTokenator : public NActors::TActorBootstrapped<TMetaTokenator> {
public:
    using TBase = NActors::TActorBootstrapped<TMetaTokenator>;

    static TMetaTokenator* CreateTokenator(const TTokensConfig& tokensConfig, TGrpc& grpc, const NActors::TActorId& httpProxy);
    TString GetToken(const TString& name);

protected:
    friend class NActors::TActorBootstrapped<TMetaTokenator>;
    static constexpr TDuration RPC_TIMEOUT = TDuration::Seconds(10);
    static constexpr TDuration PERIODIC_CHECK = TDuration::Seconds(30);
    static constexpr TDuration SUCCESS_REFRESH_PERIOD = TDuration::Hours(1);
    static constexpr TDuration ERROR_REFRESH_PERIOD = TDuration::Hours(1);

    struct TEvPrivate {
        enum EEv {
            EvRefreshToken = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvUpdateIamTokenYandex,
            EvUpdateStaticCredentialsToken,
            EvUpdateIamTokenNebius,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvRefreshToken : NActors::TEventLocal<TEvRefreshToken, EvRefreshToken> {
            TString Name;

            TEvRefreshToken(const TString& name)
                : Name(name)
            {}
        };

        template <ui32 TEventType, typename TResponse>
        struct TEvUpdateToken : NActors::TEventLocal<TEvUpdateToken<TEventType, TResponse>, TEventType> {
            TString Name;
            TString Subject;
            NYdbGrpc::TGrpcStatus Status;
            TResponse Response;

            TEvUpdateToken(const TString& name, const TString& subject, NYdbGrpc::TGrpcStatus&& status, TResponse&& response)
                : Name(name)
                , Subject(subject)
                , Status(status)
                , Response(response)
            {}
        };

        using TEvUpdateIamTokenYandex = TEvUpdateToken<EvUpdateIamTokenYandex, yandex::cloud::priv::iam::v1::CreateIamTokenResponse>;
        using TEvUpdateIamTokenNebius = TEvUpdateToken<EvUpdateIamTokenNebius, nebius::iam::v1::CreateTokenResponse>;
        using TEvUpdateStaticCredentialsToken = TEvUpdateToken<EvUpdateStaticCredentialsToken, Ydb::Auth::LoginResponse>;
    };

    TMetaTokenator(TTokensConfig tokensConfig, TGrpc& grpc, const NActors::TActorId& httpProxy);
    void Bootstrap();
    void HandlePeriodic();
    void Handle(TEvPrivate::TEvRefreshToken::TPtr event);
    void Handle(TEvPrivate::TEvUpdateIamTokenYandex::TPtr event);
    void Handle(TEvPrivate::TEvUpdateIamTokenNebius::TPtr event);
    void Handle(TEvPrivate::TEvUpdateStaticCredentialsToken::TPtr event);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvRefreshToken, Handle);
            hFunc(TEvPrivate::TEvUpdateIamTokenYandex, Handle);
            hFunc(TEvPrivate::TEvUpdateIamTokenNebius, Handle);
            hFunc(TEvPrivate::TEvUpdateStaticCredentialsToken, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandlePeriodic);
        }
    }

    struct TTokenRefreshRecord {
        TInstant RefreshTime;
        TString Name;

        bool operator <(const TTokenRefreshRecord& o) const {
            return RefreshTime > o.RefreshTime;
        }
    };

    struct TTokenConfigs {
        THashMap<TString, NMeta::TMetadataTokenInfo> MetadataTokenConfigs;
        THashMap<TString, NMeta::TJwtInfo> JwtTokenConfigs;
        THashMap<TString, NMeta::TOAuthInfo> OauthTokenConfigs;
        THashMap<TString, NMeta::TStaticCredentialsInfo> StaticCredentialsConfigs;
        THashMap<TString, NMeta::TStaticTokenInfo> StaticTokensConfigs;
        NMeta::EAccessServiceType AccessServiceType;

        const NMeta::TMetadataTokenInfo* GetMetadataTokenConfig(const TString& name);
        const NMeta::TJwtInfo* GetJwtTokenConfig(const TString& name);
        const NMeta::TOAuthInfo* GetOAuthTokenConfig(const TString& name);
        const NMeta::TStaticCredentialsInfo* GetStaticCredentialsTokenConfig(const TString& name);
        const NMeta::TStaticTokenInfo* GetStaticTokenConfig(const TString& name);

    private:
        template <typename TTokenInfo>
        const TTokenInfo* GetTokenConfig(const THashMap<TString, TTokenInfo>& configs, const TString& name) {
            auto it = configs.find(name);
            if (it != configs.end()) {
                return &it->second;
            }
            return nullptr;
        }
    };

    TPriorityQueue<TTokenRefreshRecord> RefreshQueue;
    THashMap<TString, TString> Tokens;
    TTokenConfigs TokenConfigs;
    TSpinLock TokensLock;
    TGrpc& Grpc;
    NActors::TActorId HttpProxy;
    THashMap<NHttp::THttpRequest*, TString> HttpRequestNames;

    template <typename TGRpcService>
    std::unique_ptr<TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnection(const TString& endpoint) {
        return Grpc.CreateGRpcServiceConnectionFromEndpoint<TGRpcService>(endpoint);
    }

    template <typename TGrpcService, typename TRequest, typename TResponse, typename TUpdateToken>
    void RequestCreateToken(const TString& name, const TString& endpoint, TRequest& request, typename NYdbGrpc::TSimpleRequestProcessor<typename TGrpcService::Stub, TRequest, TResponse>::TAsyncRequest asyncRequest, const TString& subject = "") {
        NActors::TActorId actorId = SelfId();
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        NYdbGrpc::TCallMeta meta;
        meta.Timeout = RPC_TIMEOUT;
        auto connection = CreateGRpcServiceConnection<TGrpcService>(endpoint);
        NYdbGrpc::TResponseCallback<TResponse> cb =
            [actorId, actorSystem, name, subject](NYdbGrpc::TGrpcStatus&& status, TResponse&& response) -> void {
                actorSystem->Send(actorId, new TUpdateToken(name, subject, std::move(status), std::move(response)));
        };
        connection->DoRequest(request, std::move(cb), asyncRequest, meta);
    }

    void UpdateMetadataToken(const NMeta::TMetadataTokenInfo* metadataTokenInfo);
    void UpdateJwtToken(const NMeta::TJwtInfo* iwtInfo);
    void UpdateOAuthToken(const NMeta::TOAuthInfo* oauthInfo);
    void UpdateStaticCredentialsToken(const NMeta::TStaticCredentialsInfo* staticCredentialsInfo);
    void UpdateStaffApiUserToken(const NMeta::TStaffApiUserTokenInfo* staffApiUserTokenInfo);
};

}
