#pragma once
#include <ydb/meta/meta.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/public/api/client/yc_private/ydb/v1/database_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/resource_preset_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/storage_type_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/console_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/operation_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/backup_service.grpc.pb.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>


namespace NMeta {

struct THandlerActorYdb {
    struct TEvPrivate {
        enum EEv {
            EvExecuteQueryResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvClusterData,
            EvJsonResponse,
            EvListAllDatabaseResponse,
            EvListStorageTypesResponse,
            EvListResourcePresetsResponse,
            EvGetConfigResponse,
            EvRetryRequest,
            EvErrorResponse,
            EvTryAgain,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvJsonResponse : NActors::TEventLocal<TEvJsonResponse, EvJsonResponse> {
            NJson::TJsonValue Json;

            TEvJsonResponse(NJson::TJsonValue&& json)
                : Json(std::move(json))
            {}

            TEvJsonResponse(const ::google::protobuf::Message& proto, const NProtobufJson::TProto2JsonConfig& config = Proto2JsonConfig) {
                try {
                    NProtobufJson::Proto2Json(proto, Json, config);
                }
                catch (const std::exception& e) {
                    Json["error"] = e.what();
                }
            }
        };

        struct TEvExecuteQueryResult : NActors::TEventLocal<TEvExecuteQueryResult, EvExecuteQueryResult> {
            NYdb::NQuery::TExecuteQueryResult Result;

            TEvExecuteQueryResult(NYdb::NQuery::TExecuteQueryResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvClusterData : NActors::TEventLocal<TEvClusterData, EvClusterData> {
            NYdb::NQuery::TExecuteQueryResult Result;

            TEvClusterData(NYdb::NQuery::TExecuteQueryResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvListAllDatabaseResponse : NActors::TEventLocal<TEvListAllDatabaseResponse, EvListAllDatabaseResponse> {
            yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse Databases;

            TEvListAllDatabaseResponse(yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse&& databases)
                : Databases(std::move(databases))
            {}
        };

        struct TEvListStorageTypesResponse : NActors::TEventLocal<TEvListStorageTypesResponse, EvListStorageTypesResponse> {
            yandex::cloud::priv::ydb::v1::ListStorageTypesResponse Response;

            TEvListStorageTypesResponse(yandex::cloud::priv::ydb::v1::ListStorageTypesResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvListResourcePresetsResponse : NActors::TEventLocal<TEvListResourcePresetsResponse, EvListResourcePresetsResponse> {
            yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse Response;

            TEvListResourcePresetsResponse(yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvGetConfigResponse : NActors::TEventLocal<TEvGetConfigResponse, EvGetConfigResponse> {
            yandex::cloud::priv::ydb::v1::GetConfigResponse Response;

            TEvGetConfigResponse(yandex::cloud::priv::ydb::v1::GetConfigResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvRetryRequest : NActors::TEventLocal<TEvRetryRequest, EvRetryRequest> {
        };

        struct TEvErrorResponse : NActors::TEventLocal<TEvErrorResponse, EvErrorResponse> {
            TString Status;
            TString Message;
            TString Details;

            TEvErrorResponse(const TString& error)
                : Status("503")
                , Message(error)
            {}

            TEvErrorResponse(const TString& status, const TString& error)
                : Status(status)
                , Message(error)
            {}

            TEvErrorResponse(const NYdbGrpc::TGrpcStatus& status) {
                switch(status.GRpcStatusCode) {
                case grpc::StatusCode::NOT_FOUND:
                    Status = "404";
                    break;
                case grpc::StatusCode::INVALID_ARGUMENT:
                    Status = "400";
                    break;
                case grpc::StatusCode::DEADLINE_EXCEEDED:
                    Status = "504";
                    break;
                case grpc::StatusCode::RESOURCE_EXHAUSTED:
                    Status = "429";
                    break;
                case grpc::StatusCode::PERMISSION_DENIED:
                    Status = "403";
                    break;
                case grpc::StatusCode::UNAUTHENTICATED:
                    Status = "401";
                    break;
                case grpc::StatusCode::INTERNAL:
                    Status = "500";
                    break;
                case grpc::StatusCode::FAILED_PRECONDITION:
                    Status = "412";
                    break;
                case grpc::StatusCode::UNAVAILABLE:
                default:
                    Status = "503";
                    break;
                }
                Message = status.Msg;
                Details = status.Details;
            }
        };

        struct TEvTryAgain : NActors::TEventLocal<TEvTryAgain, EvTryAgain> {};
    };

    static NJson::TJsonReaderConfig JsonReaderConfig;
    static NJson::TJsonWriterConfig JsonWriterConfig;
    static NProtobufJson::TJson2ProtoConfig Json2ProtoConfig;
    static NProtobufJson::TProto2JsonConfig Proto2JsonConfig;

    struct TParameters {
        bool Success;
        NHttp::TUrlParameters UrlParameters;
        NJson::TJsonValue PostData;

        TParameters(const NHttp::THttpIncomingRequestPtr& request);
        TString GetContentParameter(TStringBuf name) const;
        TString GetUrlParameter(TStringBuf name) const;
        TString operator [](TStringBuf name) const;
        void ParamsToProto(google::protobuf::Message& proto) const;
    };

    struct TRequest {
        NActors::TActorId Sender;
        NHttp::THttpIncomingRequestPtr Request;
        TParameters Parameters;

        TRequest(const NActors::TActorId& sender, const NHttp::THttpIncomingRequestPtr& request)
            : Sender(sender)
            , Request(request)
            , Parameters(request)
        {}

        TRequest(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
            : Sender(event->Sender)
            , Request(event->Get()->Request)
            , Parameters(event->Get()->Request)
        {}

        TString GetAuthToken() const;
        TString GetAuthToken(const NHttp::THeaders& headers) const;
        TString GetAuthTokenForIAM() const;
        TString GetAuthTokenForIAM(const NHttp::THeaders& headers) const;
        static void SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value);
        void ForwardHeaders(NYdbGrpc::TCallMeta& meta) const;
        void ForwardHeadersOnlyForIAM(NYdbGrpc::TCallMeta& meta) const;
        void ForwardHeader(const NHttp::THeaders& header, NYdbGrpc::TCallMeta& meta, TStringBuf name) const;
        void ForwardHeader(const NHttp::THeaders& header, NHttp::THttpOutgoingRequestPtr& request, TStringBuf name) const;
        void ForwardHeaders(NHttp::THttpOutgoingRequestPtr& request) const;
        void ForwardHeadersOnlyForIAM(NHttp::THttpOutgoingRequestPtr& request) const;
        void ReplyWithJson(const NJson::TJsonValue& json);
    };

    static TString GetDatabaseFromPath(const TString& path) {
        return TString(TStringBuf(path).RNextTok('/'));
    }

    static TDuration GetClientTimeout() {
        return TDuration::Seconds(10);
    }

    static TDuration GetTimeout() {
        return TDuration::Seconds(20);
    }

    static TDuration GetTimeout(const TRequest& request, TDuration defaultTimeout = {}) {
        TString timeout = request.Parameters["timeout"];
        if (!defaultTimeout) {
            defaultTimeout = GetTimeout();
        }
        if (timeout) {
            return TDuration::MilliSeconds(FromStringWithDefault(timeout, defaultTimeout.MilliSeconds()));
        }
        return defaultTimeout;
    }

    static TDuration GetQueryTimeout() {
        return TDuration::Minutes(10);
    }

    static bool IsRetryableError(const NYdb::TStatus& status) {
        if (status.GetStatus() == NYdb::EStatus::CLIENT_DISCOVERY_FAILED/*402010*/) {
            return true;
        }
        return false;
    }

    static bool IsRetryableError(const Ydb::Operations::Operation& operation);

    static NHttp::THttpOutgoingResponsePtr CreateStatusResponse(NHttp::THttpIncomingRequestPtr request, const NYdb::TStatus& status);
    static NHttp::THttpOutgoingResponsePtr CreateStatusResponse(NHttp::THttpIncomingRequestPtr request, const Ydb::Operations::Operation& operation);
    static NHttp::THttpOutgoingResponsePtr CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TEvPrivate::TEvErrorResponse* error);
    static NHttp::THttpOutgoingResponsePtr CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TString& error);
    static TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser);
    static TString ColumnValueToString(const NYdb::TValue& value);
    static TString ColumnValueToString(NYdb::TValueParser& valueParser);
    static NJson::TJsonValue ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser);
    static NJson::TJsonValue ColumnValueToJsonValue(NYdb::TValueParser& valueParser);
    static NJson::TJsonValue RowToJsonValue(const NYdb::TResultSet& rs, NYdb::TResultSetParser& rsParser);
    static TString GetApiUrl(TString balancer, const TString& uri);
    static bool isalnum(const TString& str);
    static bool IsValidDatabaseId(const TString& databaseId);
    static bool IsValidParameterName(const TString& param);
    static TString GetAuthHeaderValue(const TString& tokenName);
};

class THandlerActorMetaRequest : public THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaRequest>;
    using TSelf = THandlerActorMetaRequest;
    std::shared_ptr<TYdbMeta> YdbMeta;
    TRequest Request;

    THandlerActorMetaRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : YdbMeta(std::move(ydbMeta))
        , Request(event)
    {}

    void ResolveCluster(const TString& clusterName) {
        NJson::TJsonValue cluster = YdbMeta->GetCluster(clusterName);
        if (cluster.IsDefined()) {
            OnClusterReady(cluster);
            return;
        }
        TStringBuilder query;
        NYdb::TParamsBuilder params;
        query << "DECLARE $name AS Utf8;SELECT * FROM `ydb/MasterClusterExt.db` WHERE name=$name";
        params.AddParam("$name", NYdb::TValueBuilder().Utf8(clusterName).Build());
        YdbMeta->MetaDatabase->ExecuteQuery(query, params.Build()).Subscribe([actorId = TBase::SelfId()](const NYdb::NQuery::TAsyncExecuteQueryResult& result) {
            if (auto metaYdb = InstanceYdbMeta.lock()) {
                NYdb::NQuery::TAsyncExecuteQueryResult res(result);
                metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvClusterData(res.ExtractValue()));
            }
        });
    }

    void ResolveClusterName() {
        TString clusterName = Request.Parameters["cluster_name"];
        if (clusterName.empty()) {
            return ReplyBadRequest("cluster_name wasn't specified");
        }
        return ResolveCluster(clusterName);
    }

    template<typename TMessage>
    void ParsePostData(TMessage& message) {
        NProtobufJson::Json2Proto(Request.Parameters.PostData, message, Json2ProtoConfig);
    }

    void ReplyBadRequest(const TString& error) {
        NHttp::THttpOutgoingResponsePtr response;
        response = Request.Request->CreateResponseBadRequest(error, "text/plain");
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    template<typename TStatus>
    void ReplyStatus(const TStatus& status) {
        NHttp::THttpOutgoingResponsePtr response;
        response = CreateStatusResponse(Request.Request, status);
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    virtual void OnClusterReady(const NJson::TJsonValue&) {

    }

    TString GetControlPlaneEndpoint(const NJson::TJsonValue& cluster) {
        return ConstructEndpoint(cluster["control_plane"].GetStringRobust(), cluster["mvp_token"].GetStringRobust());
    }

    template<typename TRequestType>
    void PrepareControlPlaneCall(NYdbGrpc::TCallMeta& meta, TEndpointInfo& endpoint, TRequestType& controlPlaneRequest, const NJson::TJsonValue& clusterInfo) {
        if (Request.Parameters.PostData.IsDefined()) {
            ParsePostData(controlPlaneRequest); // throws exception
        }
        Request.ForwardHeaders(meta);
        meta.Timeout = GetClientTimeout();
        auto controlPlaneEndpoint = GetControlPlaneEndpoint(clusterInfo);
        endpoint = PrepareEndpoint(controlPlaneEndpoint);
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
        if constexpr (requires(TRequestType req) { req.set_location_id(""); }) {
            if (urlParams.Has("location_id")) {
                controlPlaneRequest.set_location_id(urlParams["location_id"]);
            }
        }
    }

    static NProtobufJson::TProto2JsonConfig GetOperationProto2JsonConfig() {
        return NProtobufJson::TProto2JsonConfig(THandlerActorYdb::Proto2JsonConfig)
            .SetMissingSingleKeyMode(NProtobufJson::TProto2JsonConfig::MissingKeyDefault);
    }

    NYdbGrpc::TResponseCallback<ydb::yc::priv::operation::Operation> GetOperationCallback() {
        return [actorId = SelfId()](NYdbGrpc::TGrpcStatus&& status, ydb::yc::priv::operation::Operation&& response) -> void {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                if (status.Ok()) {
                    ydbMeta->ActorSystem->Send(actorId, new TEvPrivate::TEvJsonResponse(response, GetOperationProto2JsonConfig()));
                } else {
                    ydbMeta->ActorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                }
            }
        };
    }

    template<typename ResponseType>
    NYdbGrpc::TResponseCallback<ResponseType> GetJsonResponseCallback() {
        return [actorId = SelfId()](NYdbGrpc::TGrpcStatus&& status, ResponseType&& response) -> void {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                if (status.Ok()) {
                    ydbMeta->ActorSystem->Send(actorId, new TEvPrivate::TEvJsonResponse(response));
                } else {
                    ydbMeta->ActorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                }
            }
        };
    }

    void Handle(TEvPrivate::TEvClusterData::TPtr& ev) {
        NYdb::NQuery::TExecuteQueryResult& result(ev->Get()->Result);
        if (result.IsSuccess()) {
            if (!result.GetResultSets().empty()) {
                auto resultSet = result.GetResultSet(0);
                NYdb::TResultSetParser rsParser(resultSet);
                if (rsParser.TryNextRow()) {
                    try {
                        NJson::TJsonValue cluster = RowToJsonValue(resultSet, rsParser);
                        TString clusterName = ColumnValueToString(rsParser.GetValue("name"));
                        YdbMeta->UpdateCluster(clusterName, cluster);
                        OnClusterReady(cluster);
                    }
                    catch (const std::exception& e) {
                        ReplyBadRequest(e.what());
                    }
                } else {
                    return ReplyBadRequest("Cluster not found");
                }
            } else {
                return ReplyBadRequest("Cluster not found");
            }
        } else {
            return ReplyStatus(result);
        }
    }

    virtual void Bootstrap() {
        try {
            ResolveClusterName();
        } catch (const yexception& e) {
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
            return PassAway();
        }
        Become(&TSelf::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvJsonResponse::TPtr event) {
        auto response = Request.Request->CreateResponseOK(NJson::WriteJson(event->Get()->Json, false), "application/json; charset=utf-8");
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event) {
        auto response = CreateErrorResponse(Request.Request, event->Get());
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void Timeout() {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        PassAway();
    }

    virtual STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvJsonResponse, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            hFunc(TEvPrivate::TEvClusterData, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, Timeout);
        }
    }
};

} // namespace NMeta
