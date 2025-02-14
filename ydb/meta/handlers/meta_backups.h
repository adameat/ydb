#pragma once

#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaBackupGetRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupGetRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::GetBackupRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::BackupService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::Backup>(),
            &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncGet, meta);
    }
};

class THandlerActorMetaBackupGet : public NActors::TActor<THandlerActorMetaBackupGet> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackupGet>;

    THandlerActorMetaBackupGet()
        : TBase(&THandlerActorMetaBackupGet::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupGetRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Gets backup
                description: |
                    Returns the specified backup
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/GetBackupRequest'
                responses:
                    '200':
                        description: Backup
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/Backup'
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

class THandlerActorMetaBackupListPathsRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupListPathsRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::ListPathsRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::BackupService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::ListPathsResponse>(),
            &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncListPaths, meta);
    }
};

class THandlerActorMetaBackupListPaths : public NActors::TActor<THandlerActorMetaBackupListPaths> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackupListPaths>;

    THandlerActorMetaBackupListPaths()
        : TBase(&THandlerActorMetaBackupListPaths::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupListPathsRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: List backup paths
                description: |
                    Returns list of backup paths
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/ListPathsRequest'
                responses:
                    '200':
                        description: Backup paths
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/ListPathsResponse'
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

class THandlerActorMetaBackupListRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupListRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::ListBackupsRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::BackupService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetJsonResponseCallback<yandex::cloud::priv::ydb::v1::ListBackupsResponse>(),
            &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncList, meta);
    }
};

class THandlerActorMetaBackupList : public NActors::TActor<THandlerActorMetaBackupList> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackupList>;

    THandlerActorMetaBackupList()
        : TBase(&THandlerActorMetaBackupList::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupListRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: List backups
                description: |
                    Retrieves a list of backups
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/ListBackupsRequest'
                responses:
                    '200':
                        description: Backup list
                        content:
                            application/json:
                                schema:
                                    $ref: '#/components/schemas/ListBackupsResponse'
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

class THandlerActorMetaBackupRestartRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupRestartRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::RestartBackupRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::BackupService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetOperationCallback(),
            &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncRestart, meta);
    }
};

class THandlerActorMetaBackupRestart : public NActors::TActor<THandlerActorMetaBackupRestart> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackupRestart>;

    THandlerActorMetaBackupRestart()
        : TBase(&THandlerActorMetaBackupRestart::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupRestartRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Restart backup
                description: |
                    Restarts the specified backup
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/RestartBackupRequest'
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

class THandlerActorMetaBackupDeleteRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupDeleteRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::DeleteBackupRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::BackupService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetOperationCallback(),
            &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncDelete, meta);
    }
};

class THandlerActorMetaBackupDelete : public NActors::TActor<THandlerActorMetaBackupDelete> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackupDelete>;

    THandlerActorMetaBackupDelete()
        : TBase(&THandlerActorMetaBackupDelete::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupDeleteRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Delete backup
                description: |
                    Deletes the specified backup
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/DeleteBackupRequest'
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

class THandlerActorMetaBackupRestoreRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupRestoreRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::RestoreBackupRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetOperationCallback(),
            &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncRestore, meta);
    }
};

class THandlerActorMetaBackupRestore : public NActors::TActor<THandlerActorMetaBackupRestore> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackupRestore>;

    THandlerActorMetaBackupRestore()
        : TBase(&THandlerActorMetaBackupRestore::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupRestoreRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Restore backup
                description: |
                    Restores the specified backup
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/RestoreBackupRequest'
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

class THandlerActorMetaBackupRequest : public THandlerActorMetaRequest {
public:
    using TBase = THandlerActorMetaRequest;

    THandlerActorMetaBackupRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : THandlerActorMetaRequest(std::move(ydbMeta), std::move(event))
    {}

    void OnClusterReady(const NJson::TJsonValue& cluster) override {
        yandex::cloud::priv::ydb::v1::BackupDatabaseRequest controlPlaneRequest;
        NYdbGrpc::TCallMeta meta;
        TEndpointInfo endpoint;
        PrepareControlPlaneCall(meta, endpoint, controlPlaneRequest, cluster);
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(controlPlaneRequest,
            GetOperationCallback(),
            &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncBackup, meta);
    }
};

class THandlerActorMetaBackup : public NActors::TActor<THandlerActorMetaBackup> {
public:
    using TBase = NActors::TActor<THandlerActorMetaBackup>;

    THandlerActorMetaBackup()
        : TBase(&THandlerActorMetaBackup::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaBackupRequest(std::move(ydbMeta), std::move(event)));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                summary: Backup database
                description: |
                    Backups the specified database
                tags:
                    - Backup
                requestBody:
                    required: true
                    content:
                        application/json:
                            schema:
                                $ref: '#/components/schemas/BackupDatabaseRequest'
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
