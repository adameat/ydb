#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

using namespace NKikimr;

class THandlerActorMetaDbClustersQuery : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaDbClustersQuery> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaDbClustersQuery>;
    std::shared_ptr<TYdbMeta> YdbMeta;
    TRequest Request;

    THandlerActorMetaDbClustersQuery(
            std::shared_ptr<TYdbMeta> ydbMeta,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : YdbMeta(std::move(ydbMeta))
        , Request(sender, request)
    {}

    void Bootstrap() {
        TStringBuilder query;
        NYdb::TParamsBuilder params;
        auto name = Request.Parameters["name"];
        if (name) {
            query << "DECLARE $name AS Utf8;\n";
        }
        query << "SELECT * FROM `ydb/MasterClusterExt.db`";
        if (name) {
            query << " WHERE name=$name";
            params.AddParam("$name", NYdb::TValueBuilder().Utf8(name).Build());
        }
        query << ";\n";
        YdbMeta->MetaDatabase->ExecuteQuery(query, params.Build()).Subscribe([actorId = SelfId()](const NYdb::NQuery::TAsyncExecuteQueryResult& result) {
            if (auto metaYdb = InstanceYdbMeta.lock()) {
                NYdb::NQuery::TAsyncExecuteQueryResult res(result);
                metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvExecuteQueryResult(res.ExtractValue()));
            }
        });
        Become(&THandlerActorMetaDbClustersQuery::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvExecuteQueryResult::TPtr event) {
        NYdb::NQuery::TExecuteQueryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            auto resultSet = result.GetResultSet(0);
            NJson::TJsonValue root;
            NJson::TJsonValue& clusters = root["clusters"];
            clusters.SetType(NJson::JSON_ARRAY);
            const auto& columnsMeta = resultSet.GetColumnsMeta();
            NYdb::TResultSetParser rsParser(resultSet);
            while (rsParser.TryNextRow()) {
                NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                    const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                    cluster[columnMeta.Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                }
            }
            TString body(NJson::WriteJson(root, false));
            response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void HandleTimeout() {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExecuteQueryResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaDbClusters : public NActors::TActor<THandlerActorMetaDbClusters> {
public:
    using TBase = NActors::TActor<THandlerActorMetaDbClusters>;

    THandlerActorMetaDbClusters()
        : TBase(&THandlerActorMetaDbClusters::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaDbClustersQuery(ydbMeta, event->Sender, request));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            get:
                summary: Gets clusters list from the database
                description: |
                    Gets clusters list from the database
                tags:
                    - Clusters
                parameters:
                  - name: name
                    in: query
                    description: cluster name
                    required: false
                    type: string
                responses:
                    '200':
                        description: List of clusters
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

}
