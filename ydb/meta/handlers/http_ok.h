#pragma once
#include <ydb/meta/meta.h>

namespace NMeta {

class THandlerActorHttpOk : public NActors::TActor<THandlerActorHttpOk> {
public:
    using TBase = NActors::TActor<THandlerActorHttpOk>;

    THandlerActorHttpOk()
        : TBase(&THandlerActorHttpOk::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        auto response = event->Get()->Request->CreateResponseOK("", "text/plain");
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
