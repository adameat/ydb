#pragma once
#include <ydb/meta/meta.h>
#include "common.h"
#include <ydb/meta/json/merger.h>

namespace NMeta {

using namespace NKikimr;

class THandlerActorMetaClustersQuery : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaClustersQuery> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaClustersQuery>;
    std::shared_ptr<TYdbMeta> YdbMeta;
    TRequest Request;

    THandlerActorMetaClustersQuery(
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
        query << "SELECT version_str, color_class FROM `ydb/MasterClusterVersions.db`;\n";
        YdbMeta->MetaDatabase->ExecuteQuery(query, params.Build()).Subscribe([actorId = SelfId()](const NYdb::NQuery::TAsyncExecuteQueryResult& result) {
            if (auto metaYdb = InstanceYdbMeta.lock()) {
                NYdb::NQuery::TAsyncExecuteQueryResult res(result);
                metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvExecuteQueryResult(res.ExtractValue()));
            }
        });
        Become(&THandlerActorMetaClustersQuery::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    static TJsonMapper MapCluster(const TString& name, const TString& endpoint) {
        return [name, endpoint](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& clusters = root["clusters"];
            NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
            cluster["name"] = name;
            cluster["endpoint"] = endpoint;
            cluster["cluster"] = std::move(input);
            context.Stop = true;
            return root;
        };
    }

    static TJsonFilter MapVersions(const std::map<TString, ui32> versionColorClasses) {
        return [versionColorClasses](NJson::TJsonValue& output, TJsonMergeContext&) -> void {
            auto version_str = output["version"].GetStringRobust();
            for (const auto& [version, colorClass] : versionColorClasses) {
                if (version_str.StartsWith(version)) {
                    output["version_base_color_index"] = colorClass;
                    break;
                }
            }
        };
    }

    static TJsonMapper MapSysInfo(const TString& clusterName) {
        return [clusterName](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& clusters = root["clusters"];
            NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
            cluster["name"] = clusterName;
            NJson::TJsonValue& host = input["Host"];
            if (host.GetType() == NJson::JSON_STRING) {
                cluster["hosts"][host.GetString()] = 1;
            }
            NJson::TJsonValue& version = cluster["versions"].AppendValue(NJson::TJsonValue());
            TString roleName = "compute";
            if (input.Has("Roles")) {
                NJson::TJsonValue& jsonRoles = input["Roles"];
                if (jsonRoles.GetType() == NJson::JSON_ARRAY) {
                    const auto& array = jsonRoles.GetArray();
                    if (!array.empty() && Find(array, "Storage") != array.end()) {
                        roleName = "storage";
                    }
                }
            }
            version["role"] = roleName;
            version["version"] = std::move(input["Version"]);
            version["count"] = 1;
            context.Stop = true;
            return root;
        };
    }

    static TErrorHandler Error() {
        return [](const TString& error, TStringBuf body, TStringBuf contentType) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            root["error"] = error;
            Y_UNUSED(body);
            Y_UNUSED(contentType);
            return root;
        };
    }

    static bool ReduceMapWithSum(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext&) {
        if (!output.IsDefined()) {
            output.SetType(NJson::JSON_MAP);
        }
        NJson::TJsonValue::TMapType& target(output.GetMapSafe());
        NJson::TJsonValue::TMapType& source(input.GetMapSafe());
        for (auto& pair : source) {
            target[pair.first] = target[pair.first].GetUIntegerRobust() + pair.second.GetUIntegerRobust();
        }
        return true;
    }

    void Handle(TEvPrivate::TEvExecuteQueryResult::TPtr event) {
        NYdb::NQuery::TExecuteQueryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            std::map<TString, ui32> versionColorClasses;
            {
                auto resultSet = result.GetResultSet(1);
                NYdb::TResultSetParser rsParser(resultSet);
                while (rsParser.TryNextRow()) {
                    TString version(rsParser.ColumnParser("version_str").GetUtf8());
                    ui32 colorClass(rsParser.ColumnParser("color_class").GetOptionalUint32().value_or(0));
                    versionColorClasses[version] = colorClass;
                }
            }
            TJsonMergeRules rules;
            TVector<TJsonMergePeer> peers;
            {
                auto resultSet = result.GetResultSet(0);
                NJson::TJsonValue root;
                NJson::TJsonValue& clusters = root["clusters"];
                clusters.SetType(NJson::JSON_ARRAY);
                const auto& columnsMeta = resultSet.GetColumnsMeta();
                NYdb::TResultSetParser rsParser(resultSet);
                while (rsParser.TryNextRow()) {
                    NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
                    TString name;
                    TString balancer;
                    for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                        const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                        cluster[columnMeta.Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                        if (columnMeta.Name == "name") {
                            name = cluster[columnMeta.Name].GetStringRobust();
                        }
                        if (columnMeta.Name == "balancer") {
                            balancer = cluster[columnMeta.Name].GetStringRobust();
                        }
                    }
                    if (name && balancer) {
                        TString authHeaderValue = GetAuthHeaderValue(ColumnValueToString(rsParser.GetValue("api_user_token")));
                        {
                            TJsonMergePeer& peer = peers.emplace_back();
                            peer.URL = GetApiUrl(balancer, "/cluster");
                            if (peer.URL.StartsWith("https") && !authHeaderValue.empty()) {
                                peer.Headers.Set("Authorization", authHeaderValue);
                            }
                            peer.Timeout = TDuration::Seconds(10);
                            peer.ErrorHandler = Error();
                            peer.Rules.Mappers["."] = MapCluster(name, peer.URL);
                        }
                        {
                            TJsonMergePeer& peer = peers.emplace_back();
                            peer.URL = GetApiUrl(balancer, "/sysinfo");
                            if (peer.URL.StartsWith("https") && !authHeaderValue.empty()) {
                                peer.Headers.Set("Authorization", authHeaderValue);
                            }
                            peer.Timeout = TDuration::Seconds(10);
                            peer.ErrorHandler = Error();
                            peer.Rules.Mappers[".SystemStateInfo[]"] = MapSysInfo(name);
                            peer.Rules.Reducers[".clusters"] = ReduceGroupBy("name");
                            peer.Rules.Reducers[".clusters[].name"] = ReduceWithUniqueValue();
                            peer.Rules.Reducers[".clusters[].versions"] = ReduceGroupBy("role", "version");
                            peer.Rules.Reducers[".clusters[].versions[].role"] = ReduceWithUniqueValue();
                            peer.Rules.Reducers[".clusters[].versions[].version"] = ReduceWithUniqueValue();
                            peer.Rules.Reducers[".clusters[].versions[].count"] = ReduceWithSum();
                            peer.Rules.Reducers[".clusters[].hosts"] = &ReduceMapWithSum;
                            peer.Rules.Filters[".clusters[].versions[]"] = MapVersions(versionColorClasses);
                        }
                    }

                }
                TJsonMergePeer& peer = peers.emplace_back();
                peer.Rules.Mappers[".clusters"] = MapAll();
                peer.ParsedDocument = std::move(root);
            }
            rules.Reducers[".clusters"] = ReduceGroupBy("name");
            rules.Reducers[".clusters[].name"] = ReduceWithUniqueValue();
            // TODO(xenoxeno): capture YdbMeta shared_ptr
            CreateJsonMerger(YdbMeta->HttpProxyId, Request.Sender, std::move(Request.Request), {std::move(rules)}, std::move(peers), *YdbMeta->ActorSystem, GetTimeout());
        } else {
            response = CreateStatusResponse(Request.Request, result);
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        }
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

class THandlerActorMetaClusters : public NActors::TActor<THandlerActorMetaClusters> {
public:
    using TBase = NActors::TActor<THandlerActorMetaClusters>;

    THandlerActorMetaClusters()
        : TBase(&THandlerActorMetaClusters::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                Register(new THandlerActorMetaClustersQuery(ydbMeta, event->Sender, request));
                return;
            }
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            get:
                summary: Gets clusters list from the database enriched with sysinfo
                description: |
                    Gets clusters list from the database enriched with sysinfo
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

} // namespace NMeta
