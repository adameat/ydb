#pragma once

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "grpc_log.h"

namespace NMeta {

class TGrpc {
public:
    TString CaCertificate;

    template <typename TGRpcService>
    std::unique_ptr<NMeta::TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnection(const NYdbGrpc::TGRpcClientConfig& config) {
        return std::unique_ptr<NMeta::TLoggedGrpcServiceConnection<TGRpcService>>(new NMeta::TLoggedGrpcServiceConnection<TGRpcService>(config, GetGRpcClientLow().CreateGRpcServiceConnection<TGRpcService>(config)));
    }

    template <typename TGRpcService>
    std::unique_ptr<NMeta::TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnectionFromEndpoint(const TString& endpoint) {
        TStringBuf scheme = "grpc";
        TStringBuf host;
        TStringBuf uri;
        NHttp::CrackURL(endpoint, scheme, host, uri);
        NYdbGrpc::TGRpcClientConfig config;
        config.Locator = host;
        config.EnableSsl = (scheme == "grpcs");
        if (config.EnableSsl && CaCertificate) {
            config.SslCredentials.pem_root_certs = CaCertificate;
        }
        SetGrpcKeepAlive(config);
        return CreateGRpcServiceConnection<TGRpcService>(config);
    }

private:
    NYdbGrpc::TGRpcClientLow GRpcClientLow;

    NYdbGrpc::TGRpcClientLow& GetGRpcClientLow() {
        return GRpcClientLow;
    }

    void SetGrpcKeepAlive(NYdbGrpc::TGRpcClientConfig& config) {
        config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIME_MS] = 20000;
        config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIMEOUT_MS] = 10000;
        config.IntChannelParams[GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA] = 0;
        config.IntChannelParams[GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS] = 1;
    }
};

} // namespace NMeta
