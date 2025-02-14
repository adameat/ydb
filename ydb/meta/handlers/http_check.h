#pragma once
#include <ydb/meta/meta.h>

namespace NMeta {

class THandlerActorHttpCheck : public NActors::TActor<THandlerActorHttpCheck> {
public:
    using TBase = NActors::TActor<THandlerActorHttpCheck>;

    THandlerActorHttpCheck()
        : TBase(&THandlerActorHttpCheck::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        auto response = event->Get()->Request->CreateResponseOK("ok /ping", "text/plain");
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
