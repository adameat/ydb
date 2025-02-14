#include <ydb/meta/meta.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/library/actors/http/http_static.h>
#include "http_check.h"
#include "http_static.h"
#include "http_sensors.h"
#include "meta_db_clusters.h"
#include "meta_clusters.h"
#include "meta_cp_databases.h"
#include "meta_config.h"
#include "meta_resource_presets.h"
#include "meta_storage_types.h"
#include "meta_database.h"
#include "meta_operations.h"
#include "meta_backups.h"
#include "meta_mcp_server.h"
#include "api_proxy.h"

namespace NMeta {

using NKikimr::IsMatchesWildcard;

template<typename TProtoMessage>
YAML::Node ClusterRequestProtoToYamlSchema() {
    auto schema = TProtoToYaml::ProtoToYamlSchema<TProtoMessage>();
    schema["properties"]["cluster_name"]["type"] = "string";
    schema["properties"]["cluster_name"]["required"] = true;
    return schema;
}

void TYdbMeta::SetupMetaHandlers() {
    YAML::Node swaggerPaths;
    YAML::Node swaggerComponents;
    NJson::TJsonValue capabilitiesRoot;
    NJson::TJsonValue& capabilities(capabilitiesRoot["Capabilities"]);

    auto getSwaggerYaml = [&]() {
        YAML::Node yaml;
        yaml["openapi"] = "3.0.0";
        {
            auto info = yaml["info"];
            info["version"] = "1.0.0";
            info["title"] = "YDB Meta";
            info["description"] = "YDB META API";
        }
        yaml["paths"] = swaggerPaths;
        yaml["components"] = swaggerComponents;
        return YAML::Dump(yaml);
    };

    auto getCapabilitiesSwaggerYaml = [&]() {
        return YAML::Load(R"___(
            get:
                summary: Get backend capabilities
                description: |
                    To check what backend can do
                tags:
                    - Utility
                responses:
                    '200':
                        description: All ok
                        content:
                            application/json:
                                schema:
        )___");
    };

    auto getCapabilitiesJson = [&]() {
        return NJson::WriteJson(capabilitiesRoot, false, true);
    };

    auto registerHandler = [&](NActors::TActorId proxyId, const TString& path, auto handler) {
        ActorSystem->Send(proxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(path, ActorSystem->Register(handler)));
    };

    auto registerSwaggerHandler = [&](NActors::TActorId proxyId, const TString& path, auto handler) {
        swaggerPaths[path] = handler->GetSwagger();
        registerHandler(proxyId, path, handler);
        capabilities[path] = 1; // TODO(xenoxeno): other versions could follow later
    };

    // TODO(xenoxeno): temporary root handling
    ActorSystem->Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/",
                         ActorSystem->Register(NHttp::CreateHttpStaticContentHandler(
                                                  "/", // url
                                                  "./content/", // file path
                                                  "/content/", // resource path
                                                  "index.html" // index name
                                                  )
                                              )
                         )
                     );

    ActorSystem->Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/",
                         ActorSystem->Register(NHttp::CreateHttpStaticContentHandler(
                                                  "/api/", // url
                                                  "./content/api/", // file path
                                                  "/content/api/", // resource path
                                                  "index.html" // index name
                                                  )
                                              )
                         )
                     );

    NHttp::TUrlAdapter reactUrlAdapter = [](TFsPath& url) {
        auto path = url.GetPath();
        if (IsMatchesWildcard(path, "/ui*/static/js/*")
            || IsMatchesWildcard(path, "/ui*/static/css/*")
            || IsMatchesWildcard(path, "/ui*/static/media/*")
            || IsMatchesWildcard(path, "/ui*/static/assets/fonts/*")
            || IsMatchesWildcard(path, "/ui*/static/favicon.png")) {
            auto resPos = path.find("/static/");
            if (resPos != TString::npos) {
                path = "/ui" + path.substr(resPos);
            }
        } else if (path.StartsWith("/ui") && path != "/ui/index.html") {
                path = "/ui/index.html";
        }
        if (path == "/") {
            path = "/index.html";
        }
        url = path;
    };

    ActorSystem->Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/ui/",
                         ActorSystem->Register(NHttp::CreateHttpStaticContentHandler(
                                                  "/ui/", // url
                                                  "./content/ui/", // file path
                                                  "/content/ui/", // resource path
                                                  std::move(reactUrlAdapter)
                                                  )
                                              )
                         )
                     );

    registerSwaggerHandler(HttpProxyId, "/ping", new THandlerActorHttpCheck());
    registerHandler(HttpProxyId, "/code-assist-telemetry", new THandlerActorHttpStatic(""));
    registerHandler(HttpProxyId, "/code-assist-suggestion", new THandlerActorHttpStatic(""));
    registerSwaggerHandler(HttpProxyId, "/proxy/", new THandlerActorApiProxy(HttpProxyId));
    registerSwaggerHandler(HttpProxyId, "/meta/sensors.json", new THandlerActorHttpSensors());
    registerSwaggerHandler(HttpIncomingCachedProxyId, "/meta/db_clusters", new THandlerActorMetaDbClusters());
    registerSwaggerHandler(HttpIncomingCachedProxyId, "/meta/clusters", new THandlerActorMetaClusters());
    registerSwaggerHandler(HttpIncomingCachedProxyId, "/meta/cp_databases", new THandlerActorMetaCpDatabases());
    registerSwaggerHandler(HttpProxyId, "/meta/get_config", new THandlerActorMetaConfig());
    registerSwaggerHandler(HttpProxyId, "/meta/list_resource_presets", new THandlerActorMetaListResourcePresets());
    registerSwaggerHandler(HttpProxyId, "/meta/list_storage_types", new THandlerActorMetaListStorageTypes());
    registerSwaggerHandler(HttpProxyId, "/meta/create_database", new THandlerActorMetaCreateDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/simulate_database", new THandlerActorMetaSimulateDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/update_database", new THandlerActorMetaUpdateDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/delete_database", new THandlerActorMetaDeleteDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/start_database", new THandlerActorMetaStartDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/stop_database", new THandlerActorMetaStopDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/list_operations", new THandlerActorMetaOperationsList());
    registerSwaggerHandler(HttpProxyId, "/meta/get_operation", new THandlerActorMetaOperationGet());
    registerSwaggerHandler(HttpProxyId, "/meta/cancel_operation", new THandlerActorMetaOperationCancel());
    registerSwaggerHandler(HttpProxyId, "/meta/get_backup", new THandlerActorMetaBackupGet());
    registerSwaggerHandler(HttpProxyId, "/meta/list_backups", new THandlerActorMetaBackupList());
    registerSwaggerHandler(HttpProxyId, "/meta/list_backup_paths", new THandlerActorMetaBackupListPaths());
    registerSwaggerHandler(HttpProxyId, "/meta/backup_restart", new THandlerActorMetaBackupRestart());
    registerSwaggerHandler(HttpProxyId, "/meta/backup_delete", new THandlerActorMetaBackupDelete());
    registerSwaggerHandler(HttpProxyId, "/meta/backup_restore", new THandlerActorMetaBackupRestore());
    registerSwaggerHandler(HttpProxyId, "/meta/backup_database", new THandlerActorMetaBackup());
    registerSwaggerHandler(HttpProxyId, "/meta/mcp", new THandlerActorMetaContextProxy());

    swaggerComponents["schemas"]["CreateDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::CreateDatabaseRequest>();
    swaggerComponents["schemas"]["UpdateDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::UpdateDatabaseRequest>();
    swaggerComponents["schemas"]["DeleteDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::DeleteDatabaseRequest>();
    swaggerComponents["schemas"]["StartDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::StartDatabaseRequest>();
    swaggerComponents["schemas"]["StopDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::StopDatabaseRequest>();
    swaggerComponents["schemas"]["SimulateResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::SimulateResponse>();
    swaggerComponents["schemas"]["GetConfigRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::GetConfigRequest>();
    swaggerComponents["schemas"]["GetConfigResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::GetConfigResponse>();
    swaggerComponents["schemas"]["ListResourcePresetsRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListResourcePresetsRequest>();
    swaggerComponents["schemas"]["ListResourcePresetsResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse>();
    swaggerComponents["schemas"]["ListStorageTypesRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListStorageTypesRequest>();
    swaggerComponents["schemas"]["ListStorageTypesResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListStorageTypesResponse>();
    swaggerComponents["schemas"]["Operation"] = TProtoToYaml::ProtoToYamlSchema<ydb::yc::priv::operation::Operation>();
    swaggerComponents["schemas"]["ListOperationsRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListOperationsRequest>();
    swaggerComponents["schemas"]["ListOperationsResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListOperationsResponse>();
    swaggerComponents["schemas"]["GetOperationRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::GetOperationRequest>();
    swaggerComponents["schemas"]["CancelOperationRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::CancelOperationRequest>();
    swaggerComponents["schemas"]["GetBackupRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::GetBackupRequest>();
    swaggerComponents["schemas"]["ListBackupsRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListBackupsRequest>();
    swaggerComponents["schemas"]["ListBackupsResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListBackupsResponse>();
    swaggerComponents["schemas"]["ListPathsRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListPathsRequest>();
    swaggerComponents["schemas"]["ListPathsResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::ListPathsResponse>();
    swaggerComponents["schemas"]["Backup"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::Backup>();
    swaggerComponents["schemas"]["DeleteBackupRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::DeleteBackupRequest>();
    swaggerComponents["schemas"]["RestartBackupRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::RestartBackupRequest>();
    swaggerComponents["schemas"]["RestoreBackupRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::RestoreBackupRequest>();
    swaggerComponents["schemas"]["BackupDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::BackupDatabaseRequest>();

    capabilities["/capabilities"] = 1;
    swaggerPaths["/capabilities"] = getCapabilitiesSwaggerYaml();
    registerHandler(HttpProxyId, "/capabilities", new THandlerActorHttpStatic(getCapabilitiesJson(), "application/json"));
    registerHandler(HttpProxyId, "/api/meta.yaml", new THandlerActorHttpStatic(getSwaggerYaml(), "application/yaml"));


    //ActorSystem->Send(HttpProxyId,
    //    new NHttp::TEvHttpProxy::TEvRegisterHandler("/meta/database", ActorSystem->Register(new THandlerActorYdbcDatabase())));

/*  ActorSystem.Send(HttpIncomingCachedProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cluster",
                         ActorSystem.Register(new NMVP::THandlerActorMetaCluster(HttpProxyId, MetaLocation))
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/mem_profiler",
                         ActorSystem.Register(CreateMemProfiler())
                         )
                     );


    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/mvp.json",
                         ActorSystem.Register(new NMVP::THandlerActorMvpSwagger())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/",
                         ActorSystem.Register(NHttp::CreateHttpStaticContentHandler(
                                                  "/api/", // url
                                                  "./content/api/", // file path
                                                  "/mvp/content/api/", // resource path
                                                  "index.html" // index name
                                                  )
                                              )
                         )
                     );



    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/databases",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatabases(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/database",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatabase(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/simulateDatabase",
                         actorSystem.Register(new NMVP::THandlerActorYdbcSimulateDatabase(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/operations",
                         actorSystem.Register(new NMVP::THandlerActorYdbcOperations(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/operation",
                         actorSystem.Register(new NMVP::THandlerActorYdbcOperation(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/config",
                         actorSystem.Register(new NMVP::THandlerActorYdbcConfig(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/backup",
                         actorSystem.Register(new NMVP::THandlerActorYdbcBackup(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/start_database",
                         actorSystem.Register(new NMVP::THandlerActorYdbcStart(location, httpProxyId))
                         )
                    );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/stop_database",
                         actorSystem.Register(new NMVP::THandlerActorYdbcStop(location, httpProxyId))
                         )
                    );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/restore",
                         actorSystem.Register(new NMVP::THandlerActorYdbcRestore(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/backup_list",
                         actorSystem.Register(new NMVP::THandlerActorYdbcBackupList(location, httpProxyId))
                         )
                     );
Get
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_get",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaGet(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_get_default",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaGetDefault(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_update_metric",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaUpdateMetric(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_batch_update_metric",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaBatchUpdateMetric(location, httpProxyId))
                         )
                     );
 */

}

}
