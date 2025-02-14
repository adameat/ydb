#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaConfigRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;
    ui32 Requests = 0;
    THolder<TEvPrivate::TEvListStorageTypesResponse> StorageTypesResponse;
    THolder<TEvPrivate::TEvListResourcePresetsResponse> ResourcePresetsResponse;
    THolder<TEvPrivate::TEvGetConfigResponse> GetConfigResponse;

    THandlerActorMetaConfigRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {
    }

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::GetConfigRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::GetConfigResponse> responseCb =
                [actorId = SelfId()](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::GetConfigResponse&& response) -> void {
                    if (auto ydbMeta = InstanceYdbMeta.lock()) {
                        if (status.Ok()) {
                            ydbMeta->ActorSystem->Send(actorId, new TEvPrivate::TEvGetConfigResponse(std::move(response)));
                        } else {
                            ydbMeta->ActorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                        }
                    }
            };
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::ConsoleService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::ConsoleService::Stub::AsyncGetConfig, meta);
    }

    void Handle(TEvPrivate::TEvGetConfigResponse::TPtr& event) {
        NJson::TJsonValue json;
        NProtobufJson::Proto2Json(event->Get()->Response, json, Proto2JsonConfig);
        auto response = Request.Request->CreateResponseOK(NJson::WriteJson(json, false), "application/json; charset=utf-8");
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    STATEFN(StateWork) override {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvGetConfigResponse, Handle);
        }
        return TBase::StateWork(ev);
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
        if (request->Method == "GET") {
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
            get:
                summary: Gets control plane config
                description: |
                    Gets control plane config
                tags:
                    - Databases
                parameters:
                    - in: query
                      name: cluster_name
                      required: true
                      schema:
                          type: string
                      description: Cluster name
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
