#pragma once
#include <ydb/meta/meta.h>

namespace NMeta {

class THandlerActorHttpStatic : public NActors::TActor<THandlerActorHttpStatic> {
public:
    using TBase = NActors::TActor<THandlerActorHttpStatic>;
    TString Data;
    TString ContentType;

    THandlerActorHttpStatic(const TString& data, const TString& contentType = "text/plain")
        : TBase(&THandlerActorHttpStatic::StateWork)
        , Data(data)
        , ContentType(contentType)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        auto response = event->Get()->Request->CreateResponseOK(Data, ContentType);
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
