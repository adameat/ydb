#pragma once
#include <ydb/meta/meta.h>

namespace NMeta {

class THandlerActorApiProxyRequest : public NActors::TActorBootstrapped<THandlerActorApiProxyRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorApiProxyRequest>;
    std::shared_ptr<TYdbMeta> YdbMeta;
    NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr Event;
    TString Prefix;
    TString TargetHost;
    TString Destination;

    THandlerActorApiProxyRequest(std::shared_ptr<TYdbMeta> ydbMeta, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event)
        : YdbMeta(std::move(ydbMeta))
        , Event(std::move(event))
    {}

    bool IsHostAllowed() {
        // TODO(xenoxeno): implement
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
        TStringBuf schema;
        if (host.StartsWith("https:") || host.StartsWith("http:")) {
            schema = host.NextTok(':');
        }
        TargetHost = host;
        Destination = TString("/") + destination;
        Prefix = "/proxy/host/";
        if (schema) {
            Prefix += schema;
            Prefix += ':';
        }
        Prefix += TargetHost;

        if (!IsHostAllowed()) {
            auto response = Event->Get()->Request->CreateResponse("403", "Forbidden");
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return PassAway();
        }

        NHttp::THttpOutgoingRequestPtr request = new NHttp::THttpOutgoingRequest(
            Event->Get()->Request->Method,
            schema ? schema : "http",
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
        Send(YdbMeta->HttpProxyId, requestEvent.release());

        // TODO(xenoxeno): cancelling of requests

        // TODO(xenoxeno): parse parameters for timeout
        Become(&THandlerActorApiProxyRequest::StateWork, TDuration::Seconds(120), new NActors::TEvents::TEvWakeup());
    }

    TString RewriteContentHtml(TStringBuf content, const TString& search, const TString& replace) {
        TString result;
        TString::size_type pos = 0;
        while (pos < content.size()) {
            TString::size_type start = content.find(search, pos);
            if (start == TString::npos) {
                result.append(content, pos, content.size() - pos);
                break;
            }
            result.append(content, pos, start - pos);
            result.append(replace);
            pos = start + search.size();
        }
        return result;
    }

    TString RewriteContentHtml(TStringBuf content) {
        TString result = RewriteContentHtml(content, " src='/", TStringBuilder() << " src='" << Prefix << "/");
        result = RewriteContentHtml(result, " href='/", TStringBuilder() << " href='" << Prefix << "/");
        return result;
    }

    TString RewriteContent(const TString& response) {
        NHttp::THttpResponseParser parser(response);
        NHttp::THeadersBuilder headers(parser.Headers);
        headers.Erase("Allow");
        headers.Erase("Access-Control-Allow-Origin");
        headers.Erase("Access-Control-Allow-Headers");
        headers.Erase("Access-Control-Allow-Methods");
        headers.Erase("Access-Control-Expose-Headers");
        headers.Erase("Access-Control-Allow-Credentials");
        if (headers["Content-Encoding"] == "deflate") {
            headers.Erase("Content-Encoding"); // we work with decompressed content
            headers.Erase("Content-Length"); // we will need new length after decompression
        }
        if (headers.Has("Location") && headers["Location"].StartsWith("/")) {
            headers.Set("Location", TStringBuilder() << Prefix << headers["Location"]);
        }
        headers.Set("X-Proxy-Name", YdbMeta->ExternalEndpoint);
        NHttp::THttpResponseRenderer renderer;
        renderer.InitResponse(parser.Protocol, parser.Version, parser.Status, parser.Message);
        renderer.Set(headers);
        if (parser.HasBody()) {
            if (parser.ContentType.StartsWith("text/html")) {
                renderer.SetBody(RewriteContentHtml(parser.Body));
            } else {
                renderer.SetBody(parser.Body);
            }
        }
        renderer.Finish();
        return renderer.AsString();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        if (ev->Get()->Error) {
            NHttp::THttpOutgoingResponsePtr response = Event->Get()->Request->CreateResponseServiceUnavailable(ev->Get()->Error, "text/plain");
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        } else {
            TString httpResponse = ev->Get()->Response->AsString();
            // TODO(xenoxeno): make rewrite conditional (we don't wan't to rewrite binary data)
            httpResponse = RewriteContent(httpResponse);
            // TODO(xenoxeno): process html for hyperlinks
            NHttp::THttpOutgoingResponsePtr response = Event->Get()->Request->CreateResponseString(httpResponse);
            Send(Event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        }
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
        if (auto ydbMeta = InstanceYdbMeta.lock()) {
            Register(new THandlerActorApiProxyRequest(ydbMeta, event));
        }
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