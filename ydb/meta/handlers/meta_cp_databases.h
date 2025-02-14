#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

using namespace NKikimr;

class THandlerActorMetaCpDatabasesGET : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaCpDatabasesGET> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaCpDatabasesGET>;
    std::shared_ptr<TYdbMeta> YdbMeta;
    TRequest Request;
    yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse Databases;
    NJson::TJsonValue TenantInfo;
    ui32 Requests = 0;
    TInstant DatabaseRequestDeadline;
    static constexpr TDuration MAX_DATABASE_REQUEST_TIME = TDuration::Seconds(10);
    TDuration DatabaseRequestRetryDelta = TDuration::MilliSeconds(50);
    TString ControlPlaneEndpoint;
    std::optional<TString> FilterLocationId;

    THandlerActorMetaCpDatabasesGET(
            std::shared_ptr<TYdbMeta> ydbMeta,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : YdbMeta(std::move(ydbMeta))
        , Request(sender, request)
    {}

    void Bootstrap() {
        TStringBuilder query;
        NYdb::TParamsBuilder params;
        auto name = Request.Parameters["cluster_name"];
        query << "DECLARE $name AS Utf8;SELECT * FROM `ydb/MasterClusterExt.db` WHERE name=$name";
        params.AddParam("$name", NYdb::TValueBuilder().Utf8(name).Build());
        YdbMeta->MetaDatabase->ExecuteQuery(query, params.Build()).Subscribe([actorId = SelfId()](const NYdb::NQuery::TAsyncExecuteQueryResult& result) {
            if (auto metaYdb = InstanceYdbMeta.lock()) {
                NYdb::NQuery::TAsyncExecuteQueryResult res(result);
                metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvExecuteQueryResult(res.ExtractValue()));
            }
        });
        Become(&THandlerActorMetaCpDatabasesGET::StateWork, GetTimeout(Request, TDuration::Seconds(60)), new NActors::TEvents::TEvWakeup());
    }

    void SendControlPlaneRequest() {
        yandex::cloud::priv::ydb::v1::ListAllDatabasesRequest cpRequest;
        //cpRequest.set_page_size(1000);
        cpRequest.set_database_view(yandex::cloud::priv::ydb::v1::SERVERLESS_INTERNALS);
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse> responseCb =
            [actorId = SelfId()](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse&& response) -> void {
            if (auto metaYdb = InstanceYdbMeta.lock()) {
                if (status.Ok()) {
                    metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvListAllDatabaseResponse(std::move(response)));
                } else {
                    metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                }
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeaders(meta);
        auto endpoint = PrepareEndpoint(ControlPlaneEndpoint);
        if (endpoint.TokenName) {
            TMetaTokenator* tokenator = MetaAppData()->Tokenator;
            if (tokenator) {
                TString token = tokenator->GetToken(TString(endpoint.TokenName));
                if (token) {
                    Request.SetHeader(meta, "authorization", token);
                }
            }
        }
        NHttp::TUrlParameters urlParams(endpoint.URL);
        if (urlParams.Has("location_id")) {
            FilterLocationId = urlParams["location_id"];
        }
        meta.Timeout = GetClientTimeout();
        auto connection = YdbMeta->Grpc.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(endpoint.Endpoint);
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncListAll, meta);
        ++Requests;
    }

    void Handle(TEvPrivate::TEvExecuteQueryResult::TPtr event) {
        NYdb::NQuery::TExecuteQueryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser rsParser(resultSet);
            if (rsParser.TryNextRow()) {
                TString name = ColumnValueToString(rsParser.GetValue("name"));
                TString balancer = ColumnValueToString(rsParser.GetValue("balancer"));
                TString apiUserTokenName = ColumnValueToString(rsParser.GetValue("api_user_token"));
                ControlPlaneEndpoint = ConstructEndpoint(
                    ColumnValueToString(rsParser.GetValue("control_plane")),
                    ColumnValueToString(rsParser.GetValue("mvp_token")));
                if (name && ControlPlaneEndpoint) {
                    DatabaseRequestDeadline = NActors::TActivationContext::Now() + MAX_DATABASE_REQUEST_TIME;
                    SendControlPlaneRequest();
                }
                if (balancer) {
                    TString balancerEndpoint;
                    TStringBuilder balancerEndpointBuilder;
                    balancerEndpointBuilder << "/tenantinfo";
                    if (Request.Parameters["light"] == "0") {
                        balancerEndpointBuilder << "?tablets=1";
                    } else {
                        balancerEndpointBuilder << "?tablets=0"; // default
                    }
                    if (Request.Parameters["offload"] == "1") {
                        balancerEndpointBuilder << "&offload_merge=1";
                    } else {
                        balancerEndpointBuilder << "&offload_merge=0"; // default
                    }
                    if (Request.Parameters["light"] == "0") {
                        balancerEndpointBuilder << "&storage=1&nodes=0&users=0&timeout=55000";
                    } else {
                        balancerEndpointBuilder << "&storage=0&nodes=0&users=0&timeout=55000";
                    }
                    balancerEndpoint = GetApiUrl(balancer, balancerEndpointBuilder);
                    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(balancerEndpoint);
                    TString authHeaderValue = GetAuthHeaderValue(apiUserTokenName);
                    if (balancerEndpoint.StartsWith("https") && !authHeaderValue.empty()) {
                        httpRequest->Set("Authorization", authHeaderValue);
                    }
                    THolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> request = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
                    request->Timeout = TDuration::Seconds(60);
                    Send(YdbMeta->HttpProxyId, request.Release());
                    ++Requests;
                }
            }
            if (Requests == 0) {
                ReplyAndPassAway();
            }
            return;
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void Handle(TEvPrivate::TEvRetryRequest::TPtr) {
        SendControlPlaneRequest();
        if (--Requests == 0) {
            ReplyAndPassAway();
        }
    }

    void Handle(TEvPrivate::TEvListAllDatabaseResponse::TPtr event) {
        Databases = std::move(event->Get()->Databases);
        if (--Requests == 0) {
            ReplyAndPassAway();
        }
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event) {
        if (!event->Get()->Status.StartsWith("4") && !event->Get()->Status.StartsWith("5") && DatabaseRequestDeadline > NActors::TActivationContext::Now()) {
            Schedule(DatabaseRequestRetryDelta, new TEvPrivate::TEvRetryRequest());
            DatabaseRequestRetryDelta *= 2;
            ++Requests;
        }
        if (--Requests == 0) {
            ReplyAndPassAway();
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        if (event->Get()->Error.empty() && event->Get()->Response && event->Get()->Response->Status == "200") {
            NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &TenantInfo);
        } else {
            if (event->Get()->Response && event->Get()->Response->Status.size() == 3) {
                Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(event->Get()->Response->Reverse(Request.Request)));
            } else if (!event->Get()->Error.empty()) {
                Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseServiceUnavailable(event->Get()->Error, "text/plain")));
            } else {
                Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseServiceUnavailable("Endpoint returned unknown status", "text/plain")));
            }
            return PassAway();
        }
        if (--Requests == 0) {
            ReplyAndPassAway();
        }
    }

    void HandleTimeout() {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        PassAway();
    }

    void ReplyAndPassAway() {
        NProtobufJson::TProto2JsonConfig proto2JsonConfig = NProtobufJson::TProto2JsonConfig()
                .SetMapAsObject(true)
                .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumValueMode::EnumName);

        std::unordered_map<TString, const yandex::cloud::priv::ydb::v1::Database*> indexDatabaseById;
        std::unordered_map<TString, const yandex::cloud::priv::ydb::v1::Database*> indexDatabaseByName;
        std::unordered_map<TString, NJson::TJsonValue*> indexJsonDatabaseById;

        NJson::TJsonValue root;
        BLOG_D("Received " << Databases.databases_size() << " databases from control plane");
        for (const yandex::cloud::priv::ydb::v1::Database& protoDatabase : Databases.databases()) {
            if (FilterLocationId && protoDatabase.location_id() != FilterLocationId) {
                continue;
            }
            indexDatabaseById[protoDatabase.id()] = &protoDatabase;
            indexDatabaseByName[protoDatabase.name()] = &protoDatabase;
        }
        if (FilterLocationId) {
            BLOG_D("Filtered to " << indexDatabaseById.size() << " databases from control plane");
        }
        NJson::TJsonValue& databases = root["databases"];
        databases.SetType(NJson::JSON_ARRAY);

        NJson::TJsonValue::TArray tenantArray(TenantInfo["TenantInfo"].GetArray());
        TString filterDatabase = Request.Parameters["database"];
        if (!filterDatabase) {
            std::sort(tenantArray.begin(), tenantArray.end(), [](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
                return a["Name"].GetStringRobust() < b["Name"].GetStringRobust();
            });
        }
        BLOG_D("Received " << tenantArray.size() << " databases from cluster");
        for (const NJson::TJsonValue& tenant : tenantArray) {
            if (filterDatabase && tenant["Name"].GetStringRobust() != filterDatabase) {
                continue;
            }
            NJson::TJsonValue& jsonDatabase = databases.AppendValue(NJson::TJsonValue());
            jsonDatabase = std::move(tenant);
            TString id = jsonDatabase["Id"].GetStringRobust();
            if (!id.empty()) {
                indexJsonDatabaseById[id] = &jsonDatabase;
            }
            bool foundDatabase = false;
            NJson::TJsonValue* jsonUserAttributes;
            if (jsonDatabase.GetValuePointer("UserAttributes", &jsonUserAttributes)) {
                NJson::TJsonValue* jsonDatabaseId;
                if (jsonUserAttributes->GetValuePointer("database_id", &jsonDatabaseId)) {
                    if (jsonDatabaseId->GetType() == NJson::JSON_STRING) {
                        auto itDatabase = indexDatabaseById.find(jsonDatabaseId->GetStringRobust());
                        if (itDatabase != indexDatabaseById.end()) {
                            NProtobufJson::Proto2Json(*itDatabase->second, jsonDatabase["ControlPlane"], proto2JsonConfig);
                            foundDatabase = true;
                        }
                        if (!foundDatabase) {
                            auto itDatabase = indexDatabaseByName.find(jsonDatabaseId->GetStringRobust());
                            if (itDatabase != indexDatabaseByName.end()) {
                                NProtobufJson::Proto2Json(*itDatabase->second, jsonDatabase["ControlPlane"], proto2JsonConfig);
                                foundDatabase = true;
                            }
                        }
                    }
                }
            }
            if (!foundDatabase) {
                NJson::TJsonValue* jsonName;
                if (jsonDatabase.GetValuePointer("Name", &jsonName)) {
                    if (jsonName->GetType() == NJson::JSON_STRING) {
                        auto itDatabase = indexDatabaseByName.find(jsonName->GetStringRobust());
                        if (itDatabase != indexDatabaseByName.end()) {
                            NProtobufJson::Proto2Json(*itDatabase->second, jsonDatabase["ControlPlane"], proto2JsonConfig);
                        }
                    }
                }
            }
        }

        for (const auto& [id, jsonDatabase] : indexJsonDatabaseById) {
            NJson::TJsonValue* jsonData = jsonDatabase;
            NJson::TJsonValue* jsonNodes;
            TString monitoringEndpoint;

            if (!jsonData->Has("Nodes")) {
                const NJson::TJsonValue* resourceId;
                if (jsonData->GetValuePointer("ResourceId", &resourceId)) {
                    auto itResourceDatabase = indexJsonDatabaseById.find(resourceId->GetStringRobust());
                    if (itResourceDatabase != indexJsonDatabaseById.end()) {
                        jsonData = itResourceDatabase->second;
                    }
                }
            }
            if (jsonData->GetValuePointer("Nodes", &jsonNodes)) {
                if (jsonNodes->GetType() == NJson::JSON_ARRAY) {
                    size_t size = jsonNodes->GetArray().size();
                    if (size > 0) {
                        std::random_device rd;
                        std::mt19937 gen(rd());
                        std::uniform_int_distribution<size_t> dist(0, size - 1);
                        size_t pos = dist(gen);
                        if (pos < size) {
                            const NJson::TJsonValue& jsonNode = jsonNodes->GetArray()[pos];
                            const NJson::TJsonValue* jsonEndpoints;
                            if (jsonNode.GetValuePointer("Endpoints", &jsonEndpoints)) {
                                if (jsonEndpoints->GetType() == NJson::JSON_ARRAY) {
                                    for (const auto& jsonEndpoint : jsonEndpoints->GetArray()) {
                                        if (jsonEndpoint["Name"] == "http-mon") {
                                            monitoringEndpoint = jsonNode["Host"].GetStringRobust() + jsonEndpoint["Address"].GetStringRobust();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!monitoringEndpoint.empty()) {
                (*jsonDatabase)["MonitoringEndpoint"] = monitoringEndpoint;
            }
            jsonDatabase->EraseValue("Nodes"); // to reduce response size
        }

        TString body(NJson::WriteJson(root, false));
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExecuteQueryResult, Handle);
            hFunc(TEvPrivate::TEvListAllDatabaseResponse, Handle);
            hFunc(TEvPrivate::TEvRetryRequest, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaCpDatabases : public NActors::TActor<THandlerActorMetaCpDatabases> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCpDatabases>;

    THandlerActorMetaCpDatabases()
        : TBase(&THandlerActorMetaCpDatabases::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaCpDatabasesGET(ydbMeta, event->Sender, request));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            get:
                summary: Gets databases of a cluster enriched with control plane data
                description: |
                    Gets databases of a cluster enriched with control plane data
                tags:
                    - Databases
                parameters:
                  - name: cluster_name
                    in: query
                    description: cluster name
                    required: false
                    type: string
                responses:
                    '200':
                        description: List of databases
                        content:
                            application/json:
                                schema:
                                    type: string
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
