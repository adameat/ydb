#include <ydb/meta/meta.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/library/actors/http/http_static.h>
#include "http_check.h"
#include "http_sensors.h"
#include "meta_swagger.h"
#include "meta_db_clusters.h"
#include "meta_clusters.h"
#include "meta_cp_databases.h"
#include "meta_config.h"
#include "meta_database.h"
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

    auto registerHandler = [&](NActors::TActorId proxyId, const TString& path, auto handler) {
        ActorSystem->Send(proxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(path, ActorSystem->Register(handler)));
    };

    auto registerSwaggerHandler = [&](NActors::TActorId proxyId, const TString& path, auto handler) {
        swaggerPaths[path] = handler->GetSwagger();
        registerHandler(proxyId, path, handler);
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
    registerSwaggerHandler(HttpProxyId, "/proxy/", new THandlerActorApiProxy(HttpProxyId));
    registerSwaggerHandler(HttpProxyId, "/meta/sensors.json", new THandlerActorHttpSensors());
    registerSwaggerHandler(HttpIncomingCachedProxyId, "/meta/db_clusters", new THandlerActorMetaDbClusters());
    registerSwaggerHandler(HttpIncomingCachedProxyId, "/meta/clusters", new THandlerActorMetaClusters());
    registerSwaggerHandler(HttpIncomingCachedProxyId, "/meta/cp_databases", new THandlerActorMetaCpDatabases());
    registerSwaggerHandler(HttpProxyId, "/meta/config", new THandlerActorMetaConfig());
    registerSwaggerHandler(HttpProxyId, "/meta/create_database", new THandlerActorMetaCreateDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/update_database", new THandlerActorMetaUpdateDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/delete_database", new THandlerActorMetaDeleteDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/start_database", new THandlerActorMetaStartDatabase());
    registerSwaggerHandler(HttpProxyId, "/meta/stop_database", new THandlerActorMetaStopDatabase());
    swaggerComponents["schemas"]["CreateDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::CreateDatabaseRequest>();
    swaggerComponents["schemas"]["UpdateDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::UpdateDatabaseRequest>();
    swaggerComponents["schemas"]["DeleteDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::DeleteDatabaseRequest>();
    swaggerComponents["schemas"]["StartDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::StartDatabaseRequest>();
    swaggerComponents["schemas"]["StopDatabaseRequest"] = ClusterRequestProtoToYamlSchema<yandex::cloud::priv::ydb::v1::StopDatabaseRequest>();
    swaggerComponents["schemas"]["GetConfigResponse"] = TProtoToYaml::ProtoToYamlSchema<yandex::cloud::priv::ydb::v1::GetConfigResponse>();
    swaggerComponents["schemas"]["Operation"] = TProtoToYaml::ProtoToYamlSchema<ydb::yc::priv::operation::Operation>();

    ActorSystem->Send(HttpProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/api/meta.yaml", ActorSystem->Register(new THandlerActorMetaSwagger(getSwaggerYaml()))));


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
