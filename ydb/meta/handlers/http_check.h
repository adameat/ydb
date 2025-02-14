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

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            get:
                summary: Check alive
                description: |
                    Create a new database in the YDB cluster.
                tags:
                    - Utility
                responses:
                    '200':
                        description: All ok
                        content:
                            text/plain:
                                schema:
                                    type: string
                                    example: ok /ping
        )___");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
