#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaListResourcePresetsRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaListResourcePresetsRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {
    }

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::ListResourcePresetsRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::ResourcePresetService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse>(),
            &yandex::cloud::priv::ydb::v1::ResourcePresetService::Stub::AsyncList,
            meta);
    }
};

class THandlerActorMetaListResourcePresets : public NActors::TActor<THandlerActorMetaListResourcePresets> {
public:
    using TBase = NActors::TActor<THandlerActorMetaListResourcePresets>;

    THandlerActorMetaListResourcePresets()
        : TBase(&THandlerActorMetaListResourcePresets::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET" || request->Method == "POST") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaListResourcePresetsRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Returns the list of available resource presets
                description: |
                    Returns the list of available resource presets
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/ListResourcePresetsRequest'
                responses:
                    '200':
                        description: Resource presets
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/ListResourcePresetsResponse'
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
