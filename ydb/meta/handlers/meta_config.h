#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaConfigRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaConfigRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {
    }

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::GetConfigRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::ConsoleService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::GetConfigResponse>(),
            &yandex::cloud::priv::ydb::v1::ConsoleService::Stub::AsyncGetConfig,
            meta);
    }
};

class THandlerActorMetaConfig : public NActors::TActor<THandlerActorMetaConfig> {
public:
    using TBase = NActors::TActor<THandlerActorMetaConfig>;

    THandlerActorMetaConfig()
        : TBase(&THandlerActorMetaConfig::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET" || request->Method == "POST") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaConfigRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Gets control plane config
                description: |
                    Gets control plane config
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/GetConfigRequest'
                responses:
                    '200':
                        description: Config
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/GetConfigResponse'
                    '400':
                        description: Bad request
                    '401':
                        description: Unauthorized
                    '403':
                        description: Forbidden
                    '500':
                        description: Internal server error
        )___");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
