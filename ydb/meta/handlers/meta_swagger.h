#pragma once
#include <ydb/meta/meta.h>
#include "common.h"

namespace NMeta {

class THandlerActorMetaSwagger : public NActors::TActor<THandlerActorMetaSwagger> {
public:
    using TBase = NActors::TActor<THandlerActorMetaSwagger>;
    TString SwaggerYamlText;

    THandlerActorMetaSwagger(const TString& swaggerYamlText)
        : TBase(&THandlerActorMetaSwagger::StateWork)
        , SwaggerYamlText(swaggerYamlText)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        auto response = event->Get()->Request->CreateResponseOK(SwaggerYamlText, "application/yaml");
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMeta
