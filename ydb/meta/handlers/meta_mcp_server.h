#pragma once

#include <library/cpp/string_utils/quote/quote.h>
#include <util/generic/guid.h>
#include <ydb/meta/meta.h>
#include "common.h"

template<>
class std::hash<TIntrusivePtr<NHttp::THttpOutgoingRequest>> {
public:
    size_t operator()(const TIntrusivePtr<NHttp::THttpOutgoingRequest>& request) const {
        return std::hash<void*>()(request.Get());
    }
};

namespace NMeta {

struct TEvMCP {
    enum EEv {
        // requests
        EvRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvRequest : NActors::TEventLocal<TEvRequest, EvRequest> {
        NJson::TJsonValue Request;
        TEvRequest(NJson::TJsonValue&& request)
            : Request(std::move(request))
        {}
    };
};

namespace {

TString ConvertUrlToToolName(const TString& method, const TString& url) {
    TString tool(url);
    if (tool.StartsWith("/")) {
        tool = tool.substr(1);
    }
    if (tool.StartsWith("viewer/")) {
        tool = tool.substr(7);
    }
    for (auto& ch : tool) {
        if (ch == '/') {
            ch = '-';
        }
    }
    return "ydb-" + method + "-" + tool;
}

}

struct TToolsData {
    NJson::TJsonValue Tools;

    struct TToolInfo {
        TString Method;
        TString Url;
        std::function<void (NJson::TJsonValue& params)> OnRequest;
        std::function<void (TString& body)> OnResponse;
    };

    std::unordered_map<TString, TToolInfo> ToolInfo;
};

class TMetaContextProxyRequest : public THandlerActorMetaRequest {
    using TThis = TMetaContextProxyRequest;
    using TBase = THandlerActorMetaRequest;

    NHttp::THttpOutgoingResponsePtr HttpResponse;
    TString SessionId;
    std::shared_ptr<TToolsData> Tools;
    static constexpr TDuration WakeupPeriod = TDuration::Seconds(10);
    std::unordered_map<NHttp::THttpOutgoingRequestPtr, TEvMCP::TEvRequest::TPtr> ToolsCalls;

public:
    TMetaContextProxyRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, std::shared_ptr<TToolsData> tools)
        : THandlerActorMetaRequest(std::move(ydbMeta), ev)
        , Tools(std::move(tools))
    {
    }

    TString GetLogPrefix() const {
        if (SessionId) {
            return "MCP " + SessionId + " ";
        }
        return "MCP ";
    }

    void Bootstrap() override {
        if (Request.Parameters["session_id"]) {
            auto sessionId = Request.Parameters["session_id"];
            TActorId actor = YdbMeta->FindSession(sessionId);
            if (actor == TActorId()) {
                return ReplyBadRequest("Session not found");
            }
            NHttp::THeaders headers(Request.Request->Headers);
            if (NHttp::Trim(headers.Get("Content-Type").Before(';'), ' ') != "application/json") {
                return ReplyBadRequest("Bad Content-Type");
            }
            NJson::TJsonValue jsonData;
            if (!NJson::ReadJsonTree(Request.Request->Body, &jsonData)) {
                return ReplyBadRequest("Bad Json");
            }
            auto event = std::make_unique<TEvMCP::TEvRequest>(std::move(jsonData));
            Send(actor, event.release());
            HttpResponse = Request.Request->CreateResponse("202", "Accepted", "text/plain", "Accepted");
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(HttpResponse));
            PassAway();
        } else {
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
            SessionId = CreateGuidAsString();
            YdbMeta->AddSession(SessionId, SelfId());
            HttpResponse = Request.Request->CreateIncompleteResponse("200", "OK", NHttp::THeadersBuilder({
                {"Content-Type", "text/event-stream"},
                {"Transfer-Encoding", "chunked"},
            }));
            HttpResponse->FinishHeader();
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(HttpResponse));
            {
                TStringBuilder endpoint;
                endpoint << "event: endpoint\n";
                endpoint << "data: /meta/mcp?session_id=" << SessionId << "\n\n";
                BLOG_D("Endpoint:\n" << endpoint);
                auto dataChunk = HttpResponse->CreateDataChunk(endpoint);
                Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            }
            Schedule(WakeupPeriod, new NActors::TEvents::TEvWakeup());
            Become(&TThis::StateWork);
        }
    }

    void HandleNotificationInitialized(NJson::TJsonValue& notification) {
        Y_UNUSED(notification);
    }

    void HandleInitialize(const NJson::TJsonValue&, NJson::TJsonValue& response) {
        NJson::TJsonValue& result(response["result"]);
        //result["protocolVersion"] = "2025-03-26";
        result["protocolVersion"] = "2024-11-05";
        NJson::TJsonValue& capabilities(result["capabilities"]);
        capabilities["logging"].SetType(NJson::JSON_MAP);
        capabilities["tools"]["listChanged"] = true;
        NJson::TJsonValue& serverInfo(result["serverInfo"]);
        serverInfo["name"] = "YDB";
        serverInfo["version"] = "25.1.0.0";
        result["instructions"] = R"__(
            This server allows to operate YDB database cluster.
            Cluster consist of nodes (compute and storage). They all connected to each other.
            On every cluster there are many databases.
            Database consists of compute nodes to make computations and uses storage pools to store data.
            In a database is a tree-like schema with directories, tables, topics and so on.
            Storage pool consists of storage groups.
            Each storage group consists of VDisks (virtual disks). And it uses redundancy to allow losing some of these VDisks.
            Each VDisk runs on storage node and uses part of PDisk (physical disk, located on a storage node).
            Each PDisk could contain many VDisks from different storage groups (and different storage pools).
        )__";
    }

    void HandleListTools(const NJson::TJsonValue&, NJson::TJsonValue& response) {
        NJson::TJsonValue& result(response["result"]);
        result["tools"] = Tools->Tools;
    }

    void HandleCallTools(TEvMCP::TEvRequest::TPtr& ev, NJson::TJsonValue& response) {
        NJson::TJsonValue& request(ev->Get()->Request);
        TString toolName = request["params"]["name"].GetStringRobust();
        auto it = Tools->ToolInfo.find(toolName);
        if (it == Tools->ToolInfo.end()) {
            NJson::TJsonValue id = request["id"];
            response["jsonrpc"] = "2.0";
            response["id"] = id;
            NJson::TJsonValue& result(response["result"]);
            result["isError"] = true;
            NJson::TJsonValue& content(result["content"].AppendValue({}));
            content["type"] = "text";
            content["text"] = "Tool not found";
            return;
        }
        TString method = it->second.Method;
        TString url = it->second.Url;
        BLOG_D("Calling tool " << toolName << " method=" << method << " url=" << url);
        std::vector<TString> params;
        TString clusterName;
        if (it->second.OnRequest) {
            it->second.OnRequest(request["params"]);
        }
        const NJson::TJsonValue& arguments(request["params"]["arguments"]);
        for (const auto& [name, value] : arguments.GetMap()) {
            if (name == "cluster_name") {
                clusterName = value.GetStringRobust();
                continue;
            }
            TString paramValue = value.GetStringRobust();
            Quote(paramValue);
            params.push_back(name + "=" + paramValue);
        }
        for (auto it = params.begin(); it != params.end(); ++it) {
            if (it == params.begin()) {
                url += '?';
            } else {
                url += '&';
            }
            url += *it;
        }
        if (clusterName) {
            url = YdbMeta->LocalEndpoint + "/proxy/cluster/" + clusterName + url;
            NHttp::THttpOutgoingRequestPtr httpRequest;
            if (method == "get") {
                httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);
            } else if (method == "post") {
                httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(url);
            }
            std::unique_ptr<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> httpRequestEvent(std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest));
            httpRequestEvent->AllowConnectionReuse = true;
            httpRequestEvent->Timeout = TDuration::Seconds(60);
            ToolsCalls[httpRequest] = std::move(ev);
            Send(YdbMeta->HttpProxyId, httpRequestEvent.release());
        } else {
            NJson::TJsonValue id = request["id"];
            response["jsonrpc"] = "2.0";
            response["id"] = id;
            NJson::TJsonValue& result(response["result"]);
            result["isError"] = true;
            NJson::TJsonValue& content(result["content"].AppendValue({}));
            content["type"] = "text";
            content["text"] = "Cluster name is not specified";
        }
    }

    void HandleRequest(TEvMCP::TEvRequest::TPtr& ev) {
        NJson::TJsonValue& request(ev->Get()->Request);
        BLOG_D("Request: " << NJson::WriteJson(request, false));
        if (request["jsonrpc"].GetStringRobust() != "2.0") {
            return;
        }
        TString method = request["method"].GetStringRobust();
        BLOG_D("Method " << method);
        if (method == "notification/initialized") {
            HandleNotificationInitialized(request);
        }
        NJson::TJsonValue response;
        NJson::TJsonValue id = request["id"];
        if (method == "initialize") {
            HandleInitialize(request, response);
        }
        if (method == "tools/list") {
            HandleListTools(request, response);
        }
        if (method == "tools/call") {
            HandleCallTools(ev, response);
        }
        if (response.IsDefined()) {
            response["jsonrpc"] = "2.0";
            response["id"] = id;
            TString jsonResponse = NJson::WriteJson(response, false);
            TStringBuilder data;
            data << "event: message\n";
            data << "data: " << jsonResponse << "\n\n";
            BLOG_D("Response:\n" << data);
            auto dataChunk = HttpResponse->CreateDataChunk(data);
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        auto itToolCall = ToolsCalls.find(ev->Get()->Request);
        if (itToolCall == ToolsCalls.end()) {
            BLOG_ERROR("Tool call not found");
            return;
        }
        const NJson::TJsonValue& request(itToolCall->second->Get()->Request);
        TString toolName = request["params"]["name"].GetStringRobust();
        NJson::TJsonValue response;
        NJson::TJsonValue id = request["id"];
        response["jsonrpc"] = "2.0";
        response["id"] = id;
        NJson::TJsonValue& result(response["result"]);
        NJson::TJsonValue& content(result["content"].AppendValue({}));
        content["type"] = "text";
        if (ev->Get()->Error) {
            BLOG_ERROR("Error retrieving tool " << toolName << ": " << ev->Get()->Error);
            result["isError"] = true;
            content["text"] = ev->Get()->Error;
            TString jsonResponse = NJson::WriteJson(response, false);
            TStringBuilder message;
            message << "event: message\n";
            message << "data: " << jsonResponse << "\n\n";
            auto dataChunk = HttpResponse->CreateDataChunk(message);
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            return;
        }
        if (ev->Get()->Response->Status.size() != 3 || !ev->Get()->Response->Status.starts_with("2")) {
            BLOG_ERROR("Error retrieving tool " << toolName << ": " << ev->Get()->Response->Status << " " << ev->Get()->Response->Message);
            NJson::TJsonValue& result(response["result"]);
            result["isError"] = true;
            if (ev->Get()->Response->ContentType == "text/plain" && !ev->Get()->Response->Body.empty()) {
                content["text"] = ev->Get()->Response->Body;
            } else {
                content["text"] = TStringBuilder() << ev->Get()->Response->Status << " " << ev->Get()->Response->Message;
            }
            TString jsonResponse = NJson::WriteJson(response, false);
            TStringBuilder message;
            message << "event: message\n";
            message << "data: " << jsonResponse << "\n\n";
            auto dataChunk = HttpResponse->CreateDataChunk(message);
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            return;
        }
        if (ev->Get()->Response->ContentType != "application/json") {
            BLOG_ERROR("Error retrieving tool " << toolName << ": wrong content-type =" << ev->Get()->Response->ContentType);
            NJson::TJsonValue& result(response["result"]);
            result["isError"] = true;
            content["text"] = TStringBuilder() << "wrong content-type " << ev->Get()->Response->ContentType;
            TString jsonResponse = NJson::WriteJson(response, false);
            TStringBuilder message;
            message << "event: message\n";
            message << "data: " << jsonResponse << "\n\n";
            auto dataChunk = HttpResponse->CreateDataChunk(message);
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
            return;
        }
        TString text(ev->Get()->Response->Body);
        auto it = Tools->ToolInfo.find(toolName);
        if (it != Tools->ToolInfo.end()) {
            auto& toolInfo = it->second;
            if (toolInfo.OnResponse) {
                toolInfo.OnResponse(text);
            }
        }
        content["mimeType"] = "application/json";
        content["text"] = text;
        TString jsonResponse = NJson::WriteJson(response, false);
        TStringBuilder message;
        message << "event: message\n";
        message << "data: " << jsonResponse << "\n\n";
        BLOG_D("Response:\n" << message);
        auto dataChunk = HttpResponse->CreateDataChunk(message);
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
    }

    void HandleWakeup() {
        {
            TStringBuilder ping;
            ping << ": ping - " << NActors::TActivationContext::Now().ToIsoStringLocal() << "\n\n";
            BLOG_D("Ping:\n" << ping);
            auto dataChunk = HttpResponse->CreateDataChunk(ping);
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
        }
        Schedule(WakeupPeriod, new NActors::TEvents::TEvWakeup());
    }

    void Cancelled() {
        PassAway();
    }

    void Undelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            Cancelled();
        }
    }

    void PassAway() override {
        if (SessionId) {
            YdbMeta->EndSession(SessionId, SelfId());
        }
        TBase::PassAway();
    }

    STATEFN(StateWork) override {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NActors::TEvents::TEvUndelivered, Undelivered);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
            hFunc(TEvMCP::TEvRequest, HandleRequest);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            default:
                TBase::StateWork(ev);
                break;
        }
    }

public:
    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        post:
            tags:
              - mcp
            summary: MCP
            description: MCP
            requestBody:
                description: Description
                required: false
                content:
                    application/json:
                        schema:
                            type: object
                            description: description
            responses:
                200:
                    description: OK
                    content:
                        application/json:
                            schema:
                                type: object
                                description: format depends
                        multipart/x-mixed-replace:
                            schema:
                                type: object
                                description: format depends
                        multipart/form-data:
                            schema:
                                type: object
                                description: format depends
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                504:
                    description: Gateway Timeout
            )___");
        return node;
    }
};

class THandlerActorMetaContextProxy : public NActors::TActorBootstrapped<THandlerActorMetaContextProxy>, public THandlerActorYdb {
    using TThis = THandlerActorMetaContextProxy;
    using TBase = THandlerActorMetaRequest;

    std::shared_ptr<TToolsData> Tools;

public:
    void Bootstrap() {
        ResolveMetaCluster();
        Become(&THandlerActorMetaContextProxy::StateWork);
    }

    void ResolveMetaCluster() {
        if (auto ydbMeta = InstanceYdbMeta.lock()) {
            if (ydbMeta->MetaClusterName) {
                NJson::TJsonValue cluster = ydbMeta->GetCluster(ydbMeta->MetaClusterName);
                if (cluster.IsDefined()) {
                    OnMetaClusterReady(cluster);
                    return;
                }
                TStringBuilder query;
                NYdb::TParamsBuilder params;
                query << "DECLARE $name AS Utf8;SELECT * FROM `ydb/MasterClusterExt.db` WHERE name=$name";
                params.AddParam("$name", NYdb::TValueBuilder().Utf8(ydbMeta->MetaClusterName).Build());
                ydbMeta->MetaDatabase->ExecuteQuery(query, params.Build()).Subscribe([actorId = SelfId()](const NYdb::NQuery::TAsyncExecuteQueryResult& result) {
                    if (auto metaYdb = InstanceYdbMeta.lock()) {
                        NYdb::NQuery::TAsyncExecuteQueryResult res(result);
                        metaYdb->ActorSystem->Send(actorId, new TEvPrivate::TEvClusterData(res.ExtractValue()));
                    }
                });
            }
        }
    }

    void Handle(TEvPrivate::TEvClusterData::TPtr& ev) {
        if (auto ydbMeta = InstanceYdbMeta.lock()) {
            NYdb::NQuery::TExecuteQueryResult& result(ev->Get()->Result);
            if (result.IsSuccess()) {
                if (!result.GetResultSets().empty()) {
                    auto resultSet = result.GetResultSet(0);
                    NYdb::TResultSetParser rsParser(resultSet);
                    if (rsParser.TryNextRow()) {
                        try {
                            NJson::TJsonValue cluster = RowToJsonValue(resultSet, rsParser);
                            TString clusterName = ColumnValueToString(rsParser.GetValue("name"));
                            ydbMeta->UpdateCluster(clusterName, cluster);
                            OnMetaClusterReady(cluster);
                        }
                        catch (const std::exception& e) {
                            BLOG_ERROR("Parsing meta cluster error: " << e.what());
                            Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
                        }
                    } else {
                        BLOG_ERROR("Meta cluster not found");
                        Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
                    }
                } else {
                    BLOG_ERROR("Meta cluster not found");
                    Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
                }
            } else {
                BLOG_ERROR("Error resolving meta cluster: " << result.GetIssues().ToString());
                Schedule(TDuration::Seconds(10), new TEvPrivate::TEvTryAgain());
            }
        }
    }

    void OnMetaClusterReady(const NJson::TJsonValue& cluster) {
        if (auto ydbMeta = InstanceYdbMeta.lock()) {
            TString balancer = cluster["balancer"].GetStringRobust();
            if (balancer.StartsWith("/")) {
                balancer = ydbMeta->LocalEndpoint + balancer;
            }
            if (balancer.EndsWith("/viewer/json")) {
                balancer = balancer.substr(0, balancer.length() - 12);
            }
            balancer += "/viewer/api/viewer.yaml";
            auto request = NHttp::THttpOutgoingRequest::CreateRequestGet(balancer);
            Send(ydbMeta->HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request, true));
        }
    }

    void ProcessYaml(YAML::Node yaml) {
        static const std::unordered_set<TString> SkippedPaths = {
            "/query/script/execute",
            "/query/script/fetch",
            "/viewer/check_access",
            "/viewer/topic_data",
            "/viewer/metainfo",
            "/viewer/browse",
            "/viewer/bsgroupinfo",
            "/viewer/sysinfo",
            "/viewer/nodeinfo",
            "/viewer/pdiskinfo",
            "/viewer/vdiskinfo",
            "/viewer/describe_replication",
            "/viewer/describe_topic",
            "/viewer/describe_consumer",
            "/viewer/hiveinfo",
            "/viewer/bscontrollerinfo",
            "/viewer/topicinfo",
            "/viewer/pqconsumerinfo",
            "/viewer/tabletcounters",
            "/viewer/storage_usage",
            "/viewer/labeledcounters",
            "/viewer/hivestats",
            "/viewer/netinfo",
            "/viewer/compute",
            "/viewer/graph",
            "/viewer/plan2svg",
            "/vdisk/getblob",
            "/vdisk/vdiskstat",
            "/viewer/content",
        };
        static const std::unordered_map<TString, TString> CustomName = {
            {"/viewer/tenants", "ydb-get-databases"},
            {"/viewer/tenantinfo", "ydb-get-database-info"},
        };
        static const std::unordered_map<TString, TString> CustomTitle = {
            {"ydb-get-databases", "Databases list"},
            {"ydb-get-database-info", "Databases info"},
        };
        static const std::unordered_map<TString, TString> CustomDescription = {
            {"ydb-get-databases", "Returns list of databases"},
            {"ydb-get-database-info", "Returns detailed information about databases"},
            {"ydb-get-nodes", R"__(
                Returns detailed information of nodes. It's important to specify type parameter:
                "dynamic" for compute nodes and "storage" for storage nodes.
            )__"},
            {"ydb-post-query.action", R"__(
                execute method:
                  * `execute-query` - execute query
                  * `explain-query` - explain query
            )__"},
            {"ydb-get-storage-groups.sort", "To sort ascending use one of: PoolName, Kind, MediaType, Erasure, MissingDisks, State, Usage, GroupId, Used, Limit, Usage, Available, DiskSpaceUsage, Encryption, AllocationUnits, Read, Write, Latency. Add '-' before name of field to sort descending"},
            /* {"ydb-get-scheme-directory", "Returns content of given directory in a database"},
            {"ydb-get-scheme-directory.path", "Path to directory. The path should start with database name"}, */
        };
        static const std::unordered_map<TString, std::function<void (NJson::TJsonValue&)>> CustomRequest = {
            {"ydb-get-databases", [](NJson::TJsonValue& params) {
                if (!params["arguments"].Has("state")) {
                    params["arguments"]["state"] = false;
                }
            }},
            {"ydb-get-scheme-directory", [](NJson::TJsonValue& params) {
                TString path = params["arguments"]["path"].GetStringRobust();
                TString database = params["arguments"]["database"].GetStringRobust();
                if (path.size() < database.size()) {
                    if (!database.EndsWith("/") && !path.StartsWith("/")) {
                        path = "/" + path;
                    }
                    path = database + path;
                    if (path.EndsWith("/")) {
                        path = path.substr(0, path.length() - 1);
                    }
                    params["arguments"]["path"] = path;
                }
            }},
            {"ydb-get-describe", [](NJson::TJsonValue& params) {
                TString path = params["arguments"]["path"].GetStringRobust();
                TString database = params["arguments"]["database"].GetStringRobust();
                if (path.size() < database.size()) {
                    if (!database.EndsWith("/") && !path.StartsWith("/")) {
                        path = "/" + path;
                    }
                    path = database + path;
                    if (path.EndsWith("/")) {
                        path = path.substr(0, path.length() - 1);
                    }
                    params["arguments"]["path"] = path;
                }
            }},
        };
        static const std::unordered_map<TString, std::function<void (TString&)>> CustomResponse = {
            {"ydb-get-databases", [](TString& body) {
                NJson::TJsonValue json;
                if (NJson::ReadJsonTree(body, &json)) {
                    json.InsertValue("Databases", std::move(json["Tenants"]));
                    json.EraseValue("Tenants");
                    body = NJson::WriteJson(json, false);
                }
            }},
            {"ydb-get-database-info", [](TString& body) {
                NJson::TJsonValue json;
                if (NJson::ReadJsonTree(body, &json)) {
                    json.InsertValue("DatabaseInfo", std::move(json["TenantInfo"]));
                    json.EraseValue("TenantInfo");
                    body = NJson::WriteJson(json, false);
                }
            }},
        };
        auto FillTool = [&](const TString& method, const TString& path, YAML::Node yaml, NJson::TJsonValue& jsonTool) {
            TString name;
            {
                auto it = CustomName.find(path);
                if (it != CustomName.end()) {
                    name = it->second;
                }
            }
            if (!name) {
                name = ConvertUrlToToolName(method, path);
            }
            jsonTool["name"] = name;
            {
                TString title = yaml["summary"].as<std::string>();
                {
                    auto it = CustomTitle.find(name);
                    if (it != CustomTitle.end()) {
                        title = it->second;
                    }
                }
                if (title) {
                    jsonTool["annotations"]["title"] = title;
                }
            }
            {
                TString description = yaml["description"].as<std::string>();
                {
                    auto it = CustomDescription.find(name);
                    if (it != CustomDescription.end()) {
                        description = it->second;
                    }
                }
                if (description) {
                    jsonTool["description"] = description;
                }
            }
            Tools->ToolInfo[name] = {
                .Method = method,
                .Url = path,
            };
            {
                auto it = CustomRequest.find(name);
                if (it != CustomRequest.end()) {
                    Tools->ToolInfo[name].OnRequest = it->second;
                }
            }
            {
                auto it = CustomResponse.find(name);
                if (it != CustomResponse.end()) {
                    Tools->ToolInfo[name].OnResponse = it->second;
                }
            }
            std::vector<TString> requiredParameters;
            jsonTool["inputSchema"]["type"] = "object";
            NJson::TJsonValue& jsonProperties = jsonTool["inputSchema"]["properties"];
            jsonProperties.SetType(NJson::JSON_MAP);
            if (yaml["parameters"]) {
                for (YAML::Node parameter : yaml["parameters"]) {
                    TString paramName = parameter["name"].as<std::string>();
                    NJson::TJsonValue& param(jsonProperties[paramName]);
                    if (parameter["type"]) {
                        param["type"] = parameter["type"].as<std::string>();
                    } else {
                        param["type"] = "string";
                    }
                    {
                        auto itParam = CustomDescription.find(name + "." + paramName);
                        if (itParam != CustomDescription.end()) {
                            param["description"] = itParam->second;
                        } else {
                            param["description"] = parameter["description"].as<std::string>();
                        }
                    }
                    if (parameter["required"] && parameter["required"].as<bool>()) {
                        requiredParameters.push_back(paramName);
                    }
                }
                for (const auto& name : requiredParameters) {
                    jsonTool["inputSchema"]["required"].AppendValue(name);
                }
            }
            jsonTool["annotations"]["readOnlyHint"] = true;
            jsonTool["inputSchema"]["properties"]["cluster_name"]["type"] = "string";
            jsonTool["inputSchema"]["properties"]["cluster_name"]["description"] = "cluster name";
            jsonTool["inputSchema"]["properties"]["database"]["type"] = "string";
            jsonTool["inputSchema"]["properties"]["database"]["description"] = "Database name";
            jsonTool["inputSchema"]["required"].AppendValue("cluster_name");
        };

        Tools = std::make_shared<TToolsData>();
        Tools->Tools.SetType(NJson::JSON_ARRAY);
        for (const auto& kv : yaml["paths"]) {
            auto path = kv.first.as<TString>();
            if (SkippedPaths.count(path)) {
                continue;
            }
            NJson::TJsonValue& jsonTool(Tools->Tools.AppendValue({}));
            YAML::Node data = kv.second;
            YAML::Node yamlGet = data["get"];
            if (yamlGet) {
                FillTool("get", path, yamlGet, jsonTool);
                continue;
            }
            YAML::Node yamlPost = data["post"];
            if (yamlPost) {
                FillTool("post", path, yamlPost, jsonTool);
            }
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        if (event->Get()->Error) {
            BLOG_ERROR("Error retrieving viewer.yaml: " << event->Get()->Error);
            Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
            return;
        }
        auto response = event->Get()->Response;
        if (response->Status != "200") {
            BLOG_ERROR("Error retrieving viewer.yaml: " << response->Status << " " << response->Message);
            Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
            return;
        }
        auto data = response->Body;
        if (data) {
            try {
                YAML::Node yaml = YAML::Load(TString(data));
                ProcessYaml(yaml);
                Schedule(TDuration::Hours(1), new TEvPrivate::TEvTryAgain());
            }
            catch (const std::exception& e) {
                BLOG_ERROR("Error parsing viewer.yaml: " << e.what());
                Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
                return;
            }
        } else {
            BLOG_ERROR("Empty viewer.yaml response");
            Schedule(TDuration::Seconds(60), new TEvPrivate::TEvTryAgain());
        }
    }

    void Handle(TEvPrivate::TEvTryAgain::TPtr&) {
        ResolveMetaCluster();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        if (Tools->Tools.IsDefined()) {
            if (auto ydbMeta = InstanceYdbMeta.lock()) {
                if (event->Get()->Request->GetURL().EndsWith("/tools.json")) {
                    NHttp::THttpOutgoingResponsePtr response = event->Get()->Request->CreateResponseOK(NJson::WriteJson(Tools->Tools, false), "application/json");
                    Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
                    return;
                } else {
                    Register(new TMetaContextProxyRequest(ydbMeta, event, Tools));
                }
            }
        }
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            get:
                summary: Proxies requests to a target host
                description: |
                    Proxies API calls to a target host in a form /proxy/host/{host}/{path}
                tags:
                    - Proxy
            post:
                summary: Proxies requests to a target host
                description: |
                    Proxies API calls to a target host in a form /proxy/host/{host}/{path}
                tags:
                    - Proxy
        )___");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvClusterData, Handle);
            hFunc(TEvPrivate::TEvTryAgain, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        }
    }
};

} // namespace NMeta
