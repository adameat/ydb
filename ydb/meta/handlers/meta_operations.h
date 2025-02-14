#pragma once

#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaOperationsListRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaOperationsListRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::ListOperationsRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::ConsoleService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::ListOperationsResponse>(),
            &yandex::cloud::priv::ydb::v1::ConsoleService::Stub::AsyncListOperations, meta);
    }
};

class THandlerActorMetaOperationsList : public NActors::TActor<THandlerActorMetaOperationsList> {
public:
    using TBase = NActors::TActor<THandlerActorMetaOperationsList>;

    THandlerActorMetaOperationsList()
        : TBase(&THandlerActorMetaOperationsList::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaOperationsListRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Lists operations
                description: |
                    Lists operations
                tags:
                    - Operations
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/ListOperationsRequest'
                responses:
                    '200':
                        description: List of operations
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/ListOperationsResponse'
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

class THandlerActorMetaOperationGetRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaOperationGetRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::GetOperationRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::OperationService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetOperationCallback(),
            &yandex::cloud::priv::ydb::v1::OperationService::Stub::AsyncGet, meta);
    }
};

class THandlerActorMetaOperationGet : public NActors::TActor<THandlerActorMetaOperationGet> {
public:
    using TBase = NActors::TActor<THandlerActorMetaOperationGet>;

    THandlerActorMetaOperationGet()
        : TBase(&THandlerActorMetaOperationGet::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaOperationGetRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Get operation
                description: |
                    Get operation
                tags:
                    - Operations
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/GetOperationRequest'
                responses:
                    '200':
                        description: Operation
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/Operation'
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

class THandlerActorMetaOperationCancelRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaOperationCancelRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::CancelOperationRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::OperationService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetOperationCallback(),
            &yandex::cloud::priv::ydb::v1::OperationService::Stub::AsyncCancel, meta);
    }
};

class THandlerActorMetaOperationCancel : public NActors::TActor<THandlerActorMetaOperationCancel> {
public:
    using TBase = NActors::TActor<THandlerActorMetaOperationCancel>;

    THandlerActorMetaOperationCancel()
        : TBase(&THandlerActorMetaOperationCancel::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaOperationCancelRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Cancel operation
                description: |
                    Cancels operation
                tags:
                    - Operations
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/CancelOperationRequest'
                responses:
                    '200':
                        description: Operation
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/Operation'
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
