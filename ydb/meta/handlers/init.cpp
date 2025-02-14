#include <ydb/meta/meta.h>
#include <ydb/core/util/wildcard.h>
#include <ydb/library/actors/http/http_static.h>
#include "http_check.h"
#include "http_sensors.h"
#include "meta_db_clusters.h"
#include "meta_clusters.h"
#include "meta_cp_databases.h"
#include "meta_database.h"
#include "api_proxy.h"

namespace NMeta {

using NKikimr::IsMatchesWildcard;

void TYdbMeta::SetupMetaHandlers() {
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

    ActorSystem->Send(HttpProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/ping", ActorSystem->Register(new THandlerActorHttpCheck())));

    ActorSystem->Send(HttpProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/proxy/", ActorSystem->Register(new THandlerActorApiProxy(HttpProxyId))));

    ActorSystem->Send(HttpProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/meta/sensors.json", ActorSystem->Register(new THandlerActorHttpSensors())));

    ActorSystem->Send(HttpIncomingCachedProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/meta/db_clusters", ActorSystem->Register(new THandlerActorMetaDbClusters())));

    ActorSystem->Send(HttpIncomingCachedProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/meta/clusters", ActorSystem->Register(new THandlerActorMetaClusters())));

    ActorSystem->Send(HttpIncomingCachedProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/meta/cp_databases", ActorSystem->Register(new THandlerActorMetaCpDatabases())));

    ActorSystem->Send(HttpProxyId,
        new NHttp::TEvHttpProxy::TEvRegisterHandler("/meta/create_database", ActorSystem->Register(new THandlerActorMetaCreateDatabase())));

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
