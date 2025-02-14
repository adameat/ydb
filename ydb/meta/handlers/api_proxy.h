#pragma once
#include <ydb/meta/meta.h>

namespace NMeta {

class THandlerActorApiProxyRequest : public NActors::TActorBootstrapped<THandlerActorApiProxyRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorApiProxyRequest>;
    NActors::TActorId HttpProxyId;
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    TString TargetHost;
    TString Destination;

    THandlerActorApiProxyRequest(NActors::TActorId httpProxyId, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : HttpProxyId(httpProxyId)
        , Event(std::move(event))
    {}

    bool IsHostAllowed() {
        return true;
    }

    void Bootstrap() {
        TString url = Event->Get()->Request->GetURL();
        TStringBuf destination = url;
        destination.SkipPrefix("/");
        TStringBuf proxy = destination.NextTok('/');
        if (proxy != "proxy") {
            auto response = Event->Get()->Request->CreateResponseNotFound();
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return PassAway();
        }
        TStringBuf proxy_scheme = destination.NextTok('/');
        if (proxy_scheme != "host") {
            auto response = Event->Get()->Request->CreateResponseBadRequest();
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return PassAway();
        }
        TStringBuf host = destination.NextTok('/');
        TargetHost = host;
        Destination = TString("/") + destination;

        if (!IsHostAllowed()) {
            auto response = Event->Get()->Request->CreateResponse("403", "Forbidden");
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return PassAway();
        }

        NHttp::THttpOutgoingRequestPtr request = new NHttp::THttpOutgoingRequest(
            Event->Get()->Request->Method,
            "http",
            TargetHost,
            Destination,
            Event->Get()->Request->Protocol,
            Event->Get()->Request->Version);
        NHttp::THeadersBuilder newHeaders(Event->Get()->Request->Headers);
        newHeaders.Set("Host", TargetHost);
        request->Set(newHeaders);
        if (Event->Get()->Request->Body) {
            request->SetBody(Event->Get()->Request->Body);
        }
        request->Finish();
        auto requestEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(request);
        // TODO(xenoxeno): timeout
        requestEvent->Timeout = TDuration::Seconds(120);
        requestEvent->AllowConnectionReuse = !Event->Get()->Request->IsConnectionClose();
        Send(HttpProxyId, requestEvent.release());

        // TODO(xenoxeno): cancelling of requests

        // TODO(xenoxeno): parse parameters for timeout
        Become(&THandlerActorApiProxyRequest::StateWork, TDuration::Seconds(120), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        auto response = ev->Get()->Response->Reverse(Event->Get()->Request);
        // TODO(xenoxeno): process headers for redirect location
        // TODO(xenoxeno): process html for hyperlinks
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void Timeout() {
        // TODO(xenoxeno): more detailed error
        auto response = Event->Get()->Request->CreateResponseGatewayTimeout();
        Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, Timeout);
        }
    }
};

class THandlerActorApiProxy : public NActors::TActor<THandlerActorApiProxy> {
public:
    using TBase = NActors::TActor<THandlerActorApiProxy>;
    NActors::TActorId HttpProxyId;

    THandlerActorApiProxy(NActors::TActorId httpProxyId)
        : TBase(&THandlerActorApiProxy::StateWork)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        Register(new THandlerActorApiProxyRequest(HttpProxyId, event));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            get:
                summary: Proxies requests to a target host
                description: |
                    Proxies API calls to a target host in a form /proxy/host/{host}/{path}
                tags:
                    - Utility
            post:
                summary: Proxies requests to a target host
                description: |
                    Proxies API calls to a target host in a form /proxy/host/{host}/{path}
                tags:
                    - Utility
            delete:
                summary: Proxies requests to a target host
                description: |
                    Proxies API calls to a target host in a form /proxy/host/{host}/{path}
                tags:
                    - Utility
        )___");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta