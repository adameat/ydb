#include "meta.h"
#include "log.h"
#include <google/protobuf/io/tokenizer.h>
#include <util/system/hostname.h>
#include <util/system/mlock.h>
#include <util/stream/file.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_cache.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <google/protobuf/text_format.h>
#include <ydb/meta/tokens/meta_tokens.h>

namespace NMeta {

using namespace NActors;

// TODO(xenoxeno)

//NActors::IActor* CreateMemProfiler();

const TString& GetEServiceName(NActors::NLog::EComponent component) {
    static const TString loggerName("LOGGER");
    static const TString metaName("META");
    static const TString grpcName("GRPC");
    static const TString unknownName("UNKNOW");
    switch (component) {
    case EService::Logger:
        return loggerName;
    case EService::META:
        return metaName;
    case EService::GRPC:
        return grpcName;
    default:
        return unknownName;
    }
}

TIntrusivePtr<NActors::NLog::TSettings> BuildLoggerSettings() {
    const NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = new NActors::NLog::TSettings(loggerActorId, EService::Logger, NActors::NLog::PRI_WARN);
    loggerSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name
    );
    loggerSettings->Append(
        EService::MIN,
        EService::MAX,
        GetEServiceName
    );
    TString explanation;
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NActorsServices::HTTP, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, EService::META, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, EService::GRPC, explanation);
    return loggerSettings;
}

void TYdbMeta::ParseConfig(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    TString yamlConfigPath;
    TString tokenFile;
    TString caCertificateFile;
    TString sslCertificateFile;

    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&StdErr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&MLock);

    opts.AddLongOption("config", "Path to configuration YAML file").RequiredArgument("PATH").StoreResult(&yamlConfigPath);

    opts.AddLongOption("http-port", "HTTP port. Default 8780").StoreResult(&HttpPort);
    opts.AddLongOption("https-port", "HTTPS port. Default 8781").StoreResult(&HttpsPort);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (!yamlConfigPath.empty()) {
        try {
            YAML::Node config = YAML::LoadFile(yamlConfigPath);
            auto generic = config["generic"];
            if (generic) {
                if (generic["logging"] && generic["logging"]["stderr"]) {
                    if (res.FindLongOptParseResult("stderr") == nullptr) {
                        StdErr = generic["logging"]["stderr"].as<bool>(false);
                    }
                }
                if (generic["mlock"]) {
                    if (res.FindLongOptParseResult("mlock") == nullptr) {
                        MLock = generic["mlock"].as<bool>(false);
                    }
                }
                if (generic["auth"]) {
                    auto auth = generic["auth"];
                    tokenFile = auth["token_file"].as<std::string>("");
                }
                if (generic["server"]) {
                    auto server = generic["server"];
                    caCertificateFile = server["ca_cert_file"].as<std::string>("");
                    sslCertificateFile = server["ssl_cert_file"].as<std::string>("");

                    if (res.FindLongOptParseResult("http-port") == nullptr) {
                        HttpPort = server["http_port"].as<ui16>(0);
                    }
                    if (res.FindLongOptParseResult("https-port") == nullptr) {
                        HttpsPort = server["https_port"].as<ui16>(0);
                    }
                }
            }
            auto meta = config["meta"];
            if (meta) {
                MetaCache = meta["meta_cache"].as<bool>(false);
                TString metaApiEndpoint = meta["meta_api_endpoint"].as<std::string>("");
                TString metaDatabase = meta["meta_database"].as<std::string>("");
                if (metaApiEndpoint) {
                    MetaDatabaseEndpoint = metaApiEndpoint + metaDatabase;
                } else {
                    MetaDatabaseEndpoint = metaDatabase;
                }
            }
        } catch (const YAML::Exception& e) {
            std::cerr << "Error parsing YAML configuration file: " << e.what() << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }

    if (HttpPort > 0) {
        Http = true;
    }
    if (HttpsPort > 0 || !sslCertificateFile.empty()) {
        Https = true;
    }
    if (!Http && !Https) {
        Http = true;
    }

    if (HttpPort == 0) {
        HttpPort = 8780;
    }
    if (HttpsPort == 0) {
        HttpsPort = 8781;
    }

    if (!tokenFile.empty()) {
        if (!google::protobuf::TextFormat::ParseFromString(TUnbufferedFileInput(tokenFile).ReadAll(), &TokensConfig)) {
            ythrow yexception() << "Invalid ydb token file format " << tokenFile;
        }
    }

    if (!caCertificateFile.empty()) {
        TString caCertificate = TUnbufferedFileInput(caCertificateFile).ReadAll();
        if (!caCertificate.empty()) {
            CaCertificate = caCertificate;
        } else {
            ythrow yexception() << "Invalid CA certificate file";
        }
    }
    if (!sslCertificateFile.empty()) {
        TString sslCertificate = TUnbufferedFileInput(sslCertificateFile).ReadAll();
        if (!sslCertificate.empty()) {
            SslCertificate = sslCertificate;
        } else {
            ythrow yexception() << "Invalid SSL certificate file";
        }
    }
}

void TYdbMeta::OnTerminate(int) {
    if (auto ydbMeta = InstanceYdbMeta.lock()) {
        ydbMeta->Quit = true;
    }
}

int TYdbMeta::Init(int argc, char** argv) {
    ParseConfig(argc, argv);

    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = BuildLoggerSettings();

    if (MLock) {
        LockAllMemory(LockCurrentMemory);
    }

    NActors::TLoggerActor* loggerActor = new NActors::TLoggerActor(
                BuildLoggerSettings(),
                StdErr ? NActors::CreateStderrBackend() : NActors::CreateSysLogBackend("ydb-meta", false, true),
                new NMonitoring::TDynamicCounters());
    THolder<NActors::TActorSystemSetup> setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[3]);
    setup->ExecutorsCount = 1;
    setup->Executors[0] = new NActors::TBasicExecutorPool(0, 4, 10);
    setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 100));
    setup->LocalServices.emplace_back(loggerSettings->LoggerActorId, NActors::TActorSetupCmd(loggerActor, NActors::TMailboxType::HTSwap, 0));
    setup->LocalServices.emplace_back(NActors::MakePollerActorId(), NActors::TActorSetupCmd(NActors::CreatePollerActor(), NActors::TMailboxType::HTSwap, 0));

    ActorSystem = std::make_unique<NActors::TActorSystem>(setup, &AppData, loggerSettings);
    ActorSystem->Start();
    ActorSystem->Register(NActors::CreateProcStatCollector(TDuration::Seconds(5), AppData.MetricRegistry = std::make_shared<NMonitoring::TMetricRegistry>()));

    HttpProxyId = ActorSystem->Register(NHttp::CreateHttpProxy(AppData.MetricRegistry));

    if (Http) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpPort, TStringBuilder() << FQDNHostName() << ':' << HttpPort);
        ev->CompressContentTypes = {
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/json",
        };
        ActorSystem->Send(HttpProxyId, ev);
        LocalEndpoint = TStringBuilder() << "http://[::1]:" << HttpPort;
        ExternalEndpoint = TStringBuilder() << "http://" << FQDNHostName() << ":" << HttpPort;
    }
    if (Https) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpsPort, TStringBuilder() << FQDNHostName() << ':' << HttpsPort);
        ev->Secure = true;
        ev->SslCertificatePem = SslCertificate;
        ev->CompressContentTypes = {
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/json",
        };
        ActorSystem->Send(HttpProxyId, ev);
        LocalEndpoint = TStringBuilder() << "https://[::1]:" << HttpsPort;
        ExternalEndpoint = TStringBuilder() << "https://" << FQDNHostName() << ":" << HttpsPort;
    }

    Grpc.CaCertificate = CaCertificate;

    ActorSystem->Register(AppData.Tokenator = TMetaTokenator::CreateTokenator(TokensConfig, Grpc, HttpProxyId));

    SetupCaches();
    SetupMetaHandlers();

    MetaDatabase = std::make_shared<TYdbLocation>(MetaDatabaseEndpoint);

    return 0;
}

int TYdbMeta::Run(int argc, char** argv) {
    try {
        int res = Init(argc, argv);
        if (res != 0) {
            return res;
        }
#ifndef NDEBUG
        Cout << "Started YdbMeta on " << ExternalEndpoint << Endl;
#endif
        while (!Quit) {
            Sleep(TDuration::MilliSeconds(100));
        }
#ifndef NDEBUG
        Cout << Endl << "Finished YdbMeta" << Endl;
#endif
        Shutdown();
    }
    catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
    return 0;
}

int TYdbMeta::Shutdown() {
    ActorSystem->Stop();
    return 0;
}

std::weak_ptr<TYdbMeta> InstanceYdbMeta;

} // namespace NMeta

int main(int argc, char** argv) {
    try {
        auto instance = std::make_shared<NMeta::TYdbMeta>();
        NMeta::InstanceYdbMeta = instance;
        return instance->Run(argc, argv);
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
