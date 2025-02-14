#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaCreateDatabaseRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaCreateDatabaseRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::CreateDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest, GetOperationCallback(), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncCreate, meta);
    }
};

class THandlerActorMetaCreateDatabase : public NActors::TActor<THandlerActorMetaCreateDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCreateDatabase>;

    THandlerActorMetaCreateDatabase()
        : TBase(&THandlerActorMetaCreateDatabase::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaCreateDatabaseRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Creates a new database
                description: |
                    Creates a new database in a YDB cluster
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/CreateDatabaseRequest'
                responses:
                    '200':
                        description: Database created
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

class THandlerActorMetaSimulateDatabaseRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaSimulateDatabaseRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::CreateDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::ConsoleService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::SimulateResponse>(),
            &yandex::cloud::priv::ydb::v1::ConsoleService::Stub::AsyncSimulate, meta);
    }
};

class THandlerActorMetaSimulateDatabase : public NActors::TActor<THandlerActorMetaSimulateDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorMetaSimulateDatabase>;

    THandlerActorMetaSimulateDatabase()
        : TBase(&THandlerActorMetaSimulateDatabase::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaSimulateDatabaseRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Simulates creating a new database
                description: |
                    Simulates creating of a new database in a YDB cluster
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/CreateDatabaseRequest'
                responses:
                    '200':
                        description: Database created
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/SimulateResponse'
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

class THandlerActorMetaUpdateDatabaseRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaUpdateDatabaseRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::UpdateDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest, GetOperationCallback(), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncUpdate, meta);
    }
};

class THandlerActorMetaUpdateDatabase : public NActors::TActor<THandlerActorMetaUpdateDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorMetaUpdateDatabase>;

    THandlerActorMetaUpdateDatabase()
        : TBase(&THandlerActorMetaUpdateDatabase::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaUpdateDatabaseRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Updates the specified database
                description: |
                    Updates the specified database
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/UpdateDatabaseRequest'
                responses:
                    '200':
                        description: Database updated
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

class THandlerActorMetaDeleteDatabaseRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaDeleteDatabaseRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::DeleteDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest, GetOperationCallback(), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncDelete, meta);
    }
};

class THandlerActorMetaDeleteDatabase : public NActors::TActor<THandlerActorMetaDeleteDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorMetaDeleteDatabase>;

    THandlerActorMetaDeleteDatabase()
        : TBase(&THandlerActorMetaDeleteDatabase::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaDeleteDatabaseRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Deletes the specified database
                description: |
                    Deletes the specified database
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/DeleteDatabaseRequest'
                responses:
                    '200':
                        description: Database deleted
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

class THandlerActorMetaStartDatabaseRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaStartDatabaseRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::StartDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest, GetOperationCallback(), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncStart, meta);
    }
};

class THandlerActorMetaStartDatabase : public NActors::TActor<THandlerActorMetaStartDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorMetaStartDatabase>;

    THandlerActorMetaStartDatabase()
        : TBase(&THandlerActorMetaStartDatabase::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaStartDatabaseRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Starts the specified database
                description: |
                    Starts the specified database in a YDB cluster
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/StartDatabaseRequest'
                responses:
                    '200':
                        description: Database started
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

class THandlerActorMetaStopDatabaseRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaStopDatabaseRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::StopDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest, GetOperationCallback(), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncStop, meta);
    }
};

class THandlerActorMetaStopDatabase : public NActors::TActor<THandlerActorMetaStopDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorMetaStopDatabase>;

    THandlerActorMetaStopDatabase()
        : TBase(&THandlerActorMetaStopDatabase::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaStopDatabaseRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Stops the specified database
                description: |
                    Stops the specified database in a YDB cluster
                tags:
                    - Databases
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/StopDatabaseRequest'
                responses:
                    '200':
                        description: Database stopped
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
